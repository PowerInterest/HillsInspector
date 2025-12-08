# Data Sources Overview

> **Scraper Documentation:** See `docs/scrapers/` for detailed documentation on each scraper:
> - [FEMA_FLOOD_SCRAPER.md](scrapers/FEMA_FLOOD_SCRAPER.md) - Flood zone lookup
> - [SUNBIZ_SCRAPER.md](scrapers/SUNBIZ_SCRAPER.md) - Florida LLC/Corp status
> - [REALTOR_SCRAPER.md](scrapers/REALTOR_SCRAPER.md) - Market data, HOA fees
> - [PERMIT_SCRAPER.md](scrapers/PERMIT_SCRAPER.md) - Building permits

---

## Property-Centric Storage

All scraper outputs are stored **per property** (not per scraper) to enable:
1. Re-processing screenshots with updated VisionService prompts without re-scraping
2. Deduplication - don't re-scrape if data exists and is recent
3. Centralized property data for report generation

### Filesystem Structure

```
data/properties/
├── {property_id}/                    # Usually the folio number
│   ├── screenshots/
│   │   ├── permits_20241129_143022_tampa.png
│   │   ├── realtor_20241129_143100.png
│   │   └── ...
│   ├── vision/
│   │   ├── permits_20241129_143022_v1.json
│   │   ├── realtor_20241129_143100_v1.json
│   │   └── ...
│   ├── raw/
│   │   ├── fema_20241129_143000_flood_zone.json
│   │   ├── sunbiz_20241129_143050_officer_search.json
│   │   └── ...
│   └── pdfs/
│       ├── final_judgment.pdf
│       ├── deed_2024123456.pdf
│       └── ...
```

### Database Table: `scraper_outputs`

Tracks all scraper runs for caching and re-processing:

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Primary key |
| `property_id` | VARCHAR | Property folio/ID |
| `scraper` | VARCHAR | Scraper name (fema, permits, sunbiz, realtor, etc.) |
| `scraped_at` | TIMESTAMP | When scraping occurred |
| `processed_at` | TIMESTAMP | When VisionService processed |
| `screenshot_path` | VARCHAR | Relative path to screenshot |
| `vision_output_path` | VARCHAR | Relative path to vision JSON |
| `raw_data_path` | VARCHAR | Relative path to raw data |
| `prompt_version` | VARCHAR | VisionService prompt version used |
| `extraction_success` | BOOLEAN | Whether extraction succeeded |
| `extracted_summary` | VARCHAR | JSON summary for quick access |

### Usage

```python
from src.services.scraper_storage import ScraperStorage

storage = ScraperStorage()

# Check if we need to re-scrape
if storage.needs_refresh(property_id="1234567890", scraper="permits", max_age_days=7):
    # Scrape new data
    permits = await scraper.get_permits_for_property(property_id, address, city)
else:
    # Use cached data
    cached = storage.get_latest(property_id, "permits")
    vision_data = storage.load_vision_output(property_id, cached.vision_output_path)
```

### Re-processing Screenshots

When VisionService prompts are updated, re-process existing screenshots:

```python
from src.services.scraper_storage import reprocess_screenshots

# Re-process all permit screenshots with new prompt version
reprocess_screenshots("permits", prompt_version="v2", limit=100)
```

### Scraper Integration

Each scraper has a `*_for_property()` method that uses storage:

| Scraper | Method | Cache Duration |
|---------|--------|----------------|
| FEMA | `get_flood_zone_for_property()` | 30 days |
| Sunbiz | `search_for_property()` | 30 days |
| Permits | `get_permits_for_property()` | 7 days |
| Realtor | `get_listing_for_property()` | 7 days |

## ⚠️ CRITICAL: Property Verification Required

**Legal descriptions are unreliable for identifying specific properties!**

When searching ORI by legal description (subdivision name), you will get documents for ALL properties in that subdivision, not just your target property. You MUST verify each document belongs to your target property.

### The Problem
- Searching "MUNRO AND MC INTOSH" returns documents for Lot 2 Block 6, Lot 7 Block 5, Lot 9 Block 10, etc.
- Our target might be "LOT 9 BLOCK 12" - a completely different property
- Clerks make typos: "INTOSH'S" vs "INTOSHS", "ADDN" vs "ADD" vs "ADDITION"
- Different document indexers abbreviate differently

### Verification Strategy

**After downloading documents, you MUST cross-reference using:**

1. **Folio Number** (Most reliable)
   - Extract folio from PDF text: look for "Parcel ID", "Tax Parcel", "Folio"
   - Match against HCPA bulk data folio
   - Example: `191887-0000` should match exactly

2. **Property Address** (Second most reliable)
   - Extract address from PDF: look for "Property Address", "Located at"
   - Match against known address (normalize: "205 W AMELIA AVE" = "205 WEST AMELIA AVENUE")

3. **Owner Name Cross-Reference**
   - Match PartiesOne/PartiesTwo against known owner from HCPA data
   - Current owner should appear in recent deeds

4. **Legal Description Parsing**
   - Parse LOT and BLOCK from "Legal" field
   - Only keep documents where LOT and BLOCK match target property
   - Example regex: `L(?:OT)?\s*(\d+).*B(?:LOCK)?\s*(\d+)`

### Verification Pipeline
```
1. Search ORI by subdivision name (broad search)
2. Download all potential documents
3. Extract text with Qwen-VL
4. Parse each document for: Folio, Address, Lot/Block, Owner
5. FILTER to only documents matching target property
6. Flag uncertain matches for manual review
```

---

## Data Source Roles

| Source | Primary Use | Key Data |
|--------|-------------|----------|
| **HOVER** | Final Judgment PDF | **Exact loan payout amount** from foreclosure judgment |
| **ORI** | Complete title history | Mortgages, liens, deeds, lis pendens, assignments, releases |
| **HCPA Bulk** | Property baseline | Owner, address, beds/baths, assessed value, legal description |
| **Tax Collector** | Unpaid taxes | Property tax status, certificates, amounts due |

**Note:** HOVER is for the **Final Judgment amount** (what the bank is owed). ORI is for **all recorded documents** that affect the property title (to determine what survives foreclosure).

---

## ORI (Official Records Index) - Title Documents

### ORI Search API

**Endpoint:** `POST https://publicaccess.hillsclerk.com/Public/ORIUtilities/DocumentSearch/api/Search`

**Required Headers:**
```
Content-Type: application/json; charset=UTF-8
Accept: application/json, text/javascript, */*; q=0.01
Origin: https://publicaccess.hillsclerk.com
Referer: https://publicaccess.hillsclerk.com/oripublicaccess/
X-Requested-With: XMLHttpRequest
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36
```

**Request Payload Example (search by Legal Description):**
```json
{
  "DocType": [
    "(MTG) MORTGAGE",
    "(LN) LIEN",
    "(D) DEED",
    "(LP) LIS PENDENS"
  ],
  "RecordDateBegin": "01/01/1900",
  "RecordDateEnd": "11/26/2025",
  "Legal": ["CONTAINS", "MUNRO AND MC INTOSH"]
}
```

**Legal Search Operators:**
- `["EQUALS", "EXACT TEXT"]` - Exact match
- `["CONTAINS", "PARTIAL"]` - Contains text
- `["BEGINS", "START"]` - Starts with text

**Response Structure:**
```json
{
  "Success": true,
  "ErrorMessage": null,
  "Truncated": null,
  "ResultList": [...],
  "CertificationFees": {"ClerkFee": 2, "PageFee": 1, "ECertifyFee": 6}
}
```

**ResultList Item Fields:**
| Field | Description | Example |
|-------|-------------|---------|
| `Instrument` | Unique document number | `2016469796` |
| `PartiesOne` | Array of grantor names | `["HILLSBOROUGH COUNTY CLK"]` |
| `PartiesTwo` | Array of grantee names | `["NAIM STEVEN JOSEPH"]` |
| `RecordDate` | Unix timestamp (seconds) | `1480592350` |
| `DocType` | Document type with code | `"(TAXDEED) TAX DEED"` |
| `BookType` | Book type | `"O"` (Official Records) |
| `BookNum` | Book number | `24562` |
| `PageNum` | Page number | `1619` |
| `PageCount` | Number of pages | `1` |
| `Legal` | Legal description | `"L 9 B 12 MUNRO AND MC INTOSHS ADD"` |
| `SalesPrice` | Sales price if applicable | `10020` |
| `ID` | Encoded document ID | (for PDF retrieval) |
| `UUID` | Document UUID | (alternate identifier) |

### Complete ORI Document Type List (DO NOT DELETE)

This is the complete list of all document types available in the ORI system:

```json
{
  "DocType": [
    "(AFF) AFFIDAVIT",
    "(AGR) AGREEMENT",
    "(AGD) AGREEMENT AND/OR CONTRACT FOR DEED",
    "(ASG) ASSIGNMENT",
    "(ASINT) ASSIGNMENT OF INTEREST",
    "(ASGT) ASSIGNMENT/TAXES",
    "(BND) BOND",
    "(CTF) CERTIFICATE",
    "(CCJ) CERTIFIED COPY OF A COURT JUDGMENT",
    "(CND) DECLARATION OF CONDOMINIUM",
    "(CONDO) CONDOMINIUM PLAN",
    "(LNCORPTX) CORP TAX LIEN FOR STATE OF FLORIDA",
    "(COHOME) COURT ORDER DETER HMSTD",
    "(CP) COURT PAPER",
    "(DC) DEATH CERTIFICATE",
    "(D) DEED",
    "(DPL) DEED PLAT",
    "(DRCP) DOMESTIC RELATIONS COURT PAPER",
    "(DRJUD) DOMESTIC RELATIONS JUDGMENT",
    "(EAS) EASEMENT",
    "(FIN) FINANCING STATEMENT",
    "(GOV) GOVERNMENT RELATED",
    "(JUD) JUDGMENT",
    "(LN) LIEN",
    "(LP) LIS PENDENS",
    "(MROW) MAINTAINED RIGHT OF WAY",
    "(MAR) MARRIAGE RECORD",
    "(MEDLN) MEDICAID LIEN",
    "(MIL) MILITARY DISCHARGE/SEPARATION",
    "(MSUBDSURV) MINOR SUBDIVISION SURVEY",
    "(MOD) MODIFICATION",
    "(MTG) MORTGAGE",
    "(MTGNDOC) MORTGAGE NO DOC STAMPS",
    "(MTGNT) MORTGAGE EXEMPT TAXES",
    "(MTGNIT) MORTGAGE NO INTANGIBLE TAXES",
    "(MTGREV) MORTGAGE REVERSE",
    "(NOT) NOTICE",
    "(NOC) NOTICE OF COMMENCEMENT",
    "(NCL) NOTICE OF CONTEST OF LIEN",
    "(ODNPFL) ORDER DENYING PETITION FAMILY",
    "(ODRNS) ORDER OF DISMISSAL RESPONDENT NOT SERVED",
    "(ORD) ORDER",
    "(PR) PARTIAL RELEASE",
    "(PL) PLAT",
    "(PUR) PLAT RELATED",
    "(PRO) PROBATE DOCUMENTS",
    "(POA) POWER OF ATTORNEY",
    "(PRREL) PERSONAL REP RELEASE",
    "(REL) RELEASE",
    "(RELLP) RELEASE LIS PENDENS",
    "(REQAFF) REQUEST AFFIDAVIT",
    "(RES) RESTRICTIONS",
    "(ROWM) RIGHT OF WAY MONUMENT",
    "(ROWR) RIGHT OF WAY RESERVATION",
    "(ROWT) RIGHT OF WAY TRANSFER",
    "(SAT) SATISFACTION",
    "(SATCORPTX) SATISFACTION CORP TAX FOR STATE OF FL",
    "(SLM) SURVEY & LOCATION MAP",
    "(TCDP) TAMPA CITY DOMESTIC PARTNERSHIP",
    "(TAXDEED) TAX DEED",
    "(TER) TERMINATION",
    "(TRA) TRANSFER"
  ],
  "RecordDateBegin": "08/26/1964",
  "RecordDateEnd": "11/26/2025",
  "Legal": ["BEGINS", "MUNRO AND MC INTOSH"]
}
```

**Document Types We Use for Title Search:**
```json
[
  "(MTG) MORTGAGE",
  "(MTGREV) MORTGAGE REVERSE",
  "(MTGNT) MORTGAGE EXEMPT TAXES",
  "(MTGNIT) MORTGAGE NO INTANGIBLE TAXES",
  "(LN) LIEN",
  "(MEDLN) MEDICAID LIEN",
  "(LNCORPTX) CORP TAX LIEN FOR STATE OF FLORIDA",
  "(LP) LIS PENDENS",
  "(RELLP) RELEASE LIS PENDENS",
  "(JUD) JUDGMENT",
  "(CCJ) CERTIFIED COPY OF A COURT JUDGMENT",
  "(D) DEED",
  "(ASG) ASSIGNMENT",
  "(TAXDEED) TAX DEED",
  "(SATCORPTX) SATISFACTION CORP TAX FOR STATE OF FL",
  "(SAT) SATISFACTION",
  "(REL) RELEASE",
  "(PR) PARTIAL RELEASE",
  "(NOC) NOTICE OF COMMENCEMENT",
  "(MOD) MODIFICATION",
  "(EAS) EASEMENT",
  "(ASGT) ASSIGNMENT/TAXES"
]
```

### PDF Download

**Direct PDF URL:**
```
https://publicaccess.hillsclerk.com/Public/ORIUtilities/OverlayWatermark/api/Watermark/{URL_ENCODED_ID}
```

Where `{URL_ENCODED_ID}` is the `ID` field from the search results, URL-encoded.

**Example:**
```python
from urllib.parse import quote

doc_id = "AaNZfzdHPqU40VKick6X12E1wQ0ThR14lHr1fzÁ7lojNeObcbmZHd6NTW3AvLU61ofWHsIAG0JmJktslDR2NZA8="
pdf_url = f"https://publicaccess.hillsclerk.com/Public/ORIUtilities/OverlayWatermark/api/Watermark/{quote(doc_id)}"
```

**Headers Required:**
```
Referer: https://publicaccess.hillsclerk.com/oripublicaccess/
```

**Response:** `application/pdf` - Direct PDF download with watermark

### PDF Text Extraction

**Tool:** `VisionService` (src/services/vision_service.py)
**API:** Qwen-VL at `http://10.10.1.5:6969/v1/chat/completions`
**Model:** `Qwen/Qwen3-VL-8B-Instruct`

**Process:**
1. Convert PDF to images using PyMuPDF (fitz)
2. Send each page image to Qwen-VL with extraction prompt
3. Parse extracted text for property identifiers

**Note:** We do NOT use EasyOCR. All text extraction is done via Qwen Vision API.

---

## Priority Document Types

### Priority 1: Encumbrances (What survives foreclosure?)
| Code | Type | Why We Need It |
|------|------|----------------|
| **MTG** | Mortgage | Primary debt - 1st mortgage gets wiped, junior mortgages matter |
| **MTGREV** | Reverse Mortgage | Senior liens - always survive |
| **LN** | Lien | HOA liens, mechanic's liens, judgment liens, IRS liens, property tax liens |
| **MEDLN** | Medicaid Lien | Government lien - survives foreclosure |
| **LNCORPTX** | Corp Tax Lien for State of Florida | State tax lien - survives foreclosure |

### Priority 2: Tax-Related Documents
| Code | Type | Why We Need It |
|------|------|----------------|
| **LNCORPTX** | Corp Tax Lien for State of Florida | State tax lien - ALWAYS survives |
| **SATCORPTX** | Satisfaction Corp Tax for State of FL | Tax lien paid off |
| **ASGT** | Assignment/Taxes | Tax certificate assignments |
| **TAXDEED** | Tax Deed | Prior tax sale - title issues |

### Priority 3: Foreclosure Status
| Code | Type | Why We Need It |
|------|------|----------------|
| **LP** | Lis Pendens | Foreclosure filed - establishes priority date |
| **RELLP** | Release Lis Pendens | Foreclosure dismissed/resolved |
| **JUD** | Judgment | Court judgments against property |
| **CCJ** | Certified Court Judgment | Final judgment amounts |

### Priority 4: Title Chain
| Code | Type | Why We Need It |
|------|------|----------------|
| **D** | Deed | Current ownership, prior sales |
| **ASG** | Assignment | Mortgage sold to new servicer |

### Priority 5: Releases (Debts paid off)
| Code | Type | Why We Need It |
|------|------|----------------|
| **SAT** | Satisfaction | Mortgage paid off |
| **REL** | Release | Lien released |
| **PR** | Partial Release | Portion of debt released |

### Priority 6: Other Encumbrances
| Code | Type | Why We Need It |
|------|------|----------------|
| **NOC** | Notice of Commencement | Active construction - potential mechanic's liens |
| **MOD** | Modification | Loan modification - changes debt amount |
| **EAS** | Easement | Property restrictions |

---

## Other Data Sources

### 1. Auction Sources (Where we find properties)

| Source | URL | Data Collected |
|--------|-----|----------------|
| **Foreclosure Auctions** | hillsborough.realforeclose.com | Case number, parcel ID, address, judgment amount, auction date |
| **Tax Deed Auctions** | hillsborough.realtaxdeed.com | Case number, certificate #, parcel ID, opening bid, auction date |

### 2. Property Data (Parcel details)

| Source | URL | Data Collected |
|--------|-----|----------------|
| **HCPA Bulk Download** | downloads.hcpafl.org | 528K parcels: owner, address, beds/baths, year built, assessed/market value, lot size, sale history, **LEGAL DESCRIPTION**, **FOLIO** |
| **HCPA GIS API** | gis.hcpafl.org/propertysearch | Live property search, photos, zoning, spatial data |

**Key Fields from HCPA for Verification:**
- `folio` - Primary key for property identification
- `raw_legal1`, `raw_legal2` - Legal description to match against ORI documents
- `situs_address` - Property address
- `owner_name` - Current owner

**HCPA GIS URL Format from PIN/Folio**
- Bulk PIN example: `A-13-29-18-4XZ-000012-00009.0`
- GIS URL suffix: `1829134XZ000012000090A`
- Transform: reorder Section-Township-Range to Range-Township-Section, drop dashes/decimal, move leading letter to end, then concatenate subdivision/block/lot: `13-29-18` → `182913`, `4XZ-000012-00009.0` → `4XZ000012000090`, add trailing `A`.
- URL template: `https://gis.hcpafl.org/propertysearch/#/parcel/basic/{suffix}`
- Parser/scraper: `src/scrapers/hcpa_gis_scraper.py` (`scrape_hcpa_property` handles this page and supports parcel/folio inputs).
- Sales History workflow: the GIS page exposes a Sales History table (book/page/instrument). Treat this as the authoritative feed—capture every row plus the legal description. Follow each linked document (PAVDirectSearch link) and download the PDFs; send them through Qwen OCR (`src/services/vision_service.py`) and store text+metadata per property in the DB.
- Direct document lookup: if you have an instrument number you can fetch via `https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=320&OBKey__1006_1=<INSTRUMENT>` (example: `…&OBKey__1006_1=2015177688`). We still need to map CQID/OBKey patterns and any book/page lookup syntax—capture examples and investigate as we collect more links.
- Tax and notices: from the GIS page, capture the Tax Collector link for the parcel and the linked TRIM notice PDF; OCR/store the TRIM details with the property records.
- Legal description: prefer the GIS page’s legal description over bulk data; use it to drive a legal-description ORI search for liens/encumbrances.

### 3. Lien & Title Research (Critical for equity analysis)

| Source | URL | Data Collected |
|--------|-----|----------------|
| **Official Records (ORI)** | publicaccess.hillsclerk.com/oripublicaccess | Mortgages, liens, deeds, lis pendens, assignments, releases, PDF documents |
| **Court Records (HOVER)** | hover.hillsclerk.com | Final Judgment PDFs, case dockets, foreclosure details |

---

## OnBase Direct Search Endpoints (PAVDirectSearch)

The main ORI search page (`/oripublicaccess/`) is heavily rate-limited and uses complex dynamic loading (iframes, `jsgrid`). We bypass this by using **Direct Search Endpoints** (`PAVDirectSearch`) identified by specific `CQID` (Custom Query ID) parameters.

> **Backend:** Hyland OnBase Public Sector Constituency Web Access
> - `CQID` = Custom Query ID (Configured in OnBase Studio)
> - `OBKey__<ID>_1` = Dynamic Keyword Value for Keyword Type `<ID>`
> - Mapping: `obpa_kw_486` → `OBKey__486_1`

### CQID Reference Table

| CQID | Search Type | Key Parameters | Notes |
|------|-------------|----------------|-------|
| **319** | **Book/Page** | `OBKey__573_1` (Book), `OBKey__1049_1` (Page) | **Primary.** Find deeds/mortgages from HCPA Sales History. |
| **320** | **Instrument #** | `OBKey__1006_1` (Instrument) | **Direct lookup.** Used by auction Case# links. |
| **321** | **Legal Description** | `OBKey__1011_1` (Legal) | **Fallback.** Supports wildcards (`*`). ~100 row limit. |
| **326** | **Name (Cross-Party)** | `OBKey__486_1` (Name) | **Best for Owners.** Returns rich table. |
| **318** | **Marriage Records** | `OBKey__486_1` (Name) | Name change detection. |
| **316** | **Master Search** | Various | Complex virtualized grid - avoid. |
| **324-348** | **Court Cases** | `OBKey__106_1` (Year), `OBKey__107_1` (Seq) | Future use for Foreclosure/Probate. |

### Example URLs

```
# Book/Page Search (CQID 319)
https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=319&OBKey__1530_1=O&OBKey__573_1=23264&OBKey__1049_1=1344

# Instrument Search (CQID 320)
https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=320&OBKey__1006_1=2015177688

# Legal Description Search (CQID 321) - Use prefix wildcard
https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=321&OBKey__1011_1=RIVER%20BEND*
```

### Legal Description Search (CQID 321) - Important Notes

| Parameter | Description |
|-----------|-------------|
| `OBKey__1011_1` | Legal description. Supports wildcards (`*`). |
| `OBKey__1285_1` | Document type filter (e.g., `DEED`, `MORTGAGE`). |
| `OBKey__date_from` / `OBKey__date_to` | Date range. |

**Search Strategy:**
1. **Exact matches often fail** - Clerk abbreviates differently than HCPA
2. **Use prefix wildcards** - `RIVER BEND*` works; `*RIVER BEND*` causes timeouts
3. **Result truncation** - Server limits to ~100 rows. Be specific (e.g., `RIVER BEND PH 3B*`)

**Search Priority Order:**
1. **Primary:** Book/Page Search (CQID 319) via `hcpa_gis_scraper.py`
2. **Secondary:** Name Search (CQID 326) via `ori_scraper.py`
3. **Tertiary:** Legal Description Search (CQID 321) - only when others fail

### Chain of Title Analysis Flow

1. **Seed Document** - Get Book/Page from HCPA Sales History → CQID 319
2. **Owner Identification** - Parse Grantor/Grantee from seed documents
3. **Name Change Detection** - Search marriage records (CQID 318) for aliases
4. **Deep Scan** - For each owner/alias, search CQID 326 for ALL documents
5. **Analysis** - `TitleChainService` categorizes deeds, mortgages, liens, NOCs

### Notice of Commencement (NOC) Importance

- **What:** Document recorded before construction (roof, pool, remodel)
- **Why it matters:**
  1. Every NOC should have a Building Permit - if missing = unpermitted work
  2. Open NOCs can lead to Mechanic's Liens
  3. Establishes timeline for major property improvements

### 4. Tax Status

| Source | URL | Data Collected | Scraper |
|--------|-----|----------------|---------|
| **Tax Collector** | hillsborough.county-taxes.com/public | Unpaid taxes, tax lien status, amount due | `tax_scraper.py` (stub) |

### 5. Market Valuation (ARV estimates)

| Source | URL | Data Collected | Scraper |
|--------|-----|----------------|---------|
| **Zillow** | zillow.com | Zestimate, rent estimate, price history, listing status | `market_scraper.py` |
| **Realtor.com** | realtor.com | Photos, HOA fees, price history, agent remarks | `realtor_scraper.py` |

### 6. Building Permits & Violations

| Source | URL | Data Collected | Scraper |
|--------|-----|----------------|---------|
| **City of Tampa Permits** | aca-prod.accela.com/TAMPA/Default.aspx | Permits, code violations, estimated costs | `permit_scraper.py` |
| **County Permits** | aca-prod.accela.com/HCFL/Default.aspx | Permits for unincorporated areas | `permit_scraper.py` |

### 7. Flood Risk

| Source | URL | Data Collected | Scraper |
|--------|-----|----------------|---------|
| **FEMA NFHL** | hazards.fema.gov/gis/nfhl/rest/services/public/NFHL/MapServer | Flood zone (AE, X, VE), BFE, risk level | `fema_flood_scraper.py` |

### 8. Business Entity Lookup (Owner Research)

| Source | URL | Data Collected | Scraper |
|--------|-----|----------------|---------|
| **Florida Sunbiz** | search.sunbiz.org | LLC/Corp status, officers, registered agent, filing date | `sunbiz_scraper.py` |

---

## API Reference

### FEMA Flood Zone API

**No authentication required.** Uses ArcGIS REST API.

**Endpoint:** `https://hazards.fema.gov/gis/nfhl/rest/services/public/NFHL/MapServer/28/query`

**Query by coordinates:**
```
?where=1=1
&geometry={longitude},{latitude}
&geometryType=esriGeometryPoint
&inSR=4326
&spatialRel=esriSpatialRelWithin
&outFields=FLD_ZONE,ZONE_SUBTY,SFHA_TF,STATIC_BFE
&returnGeometry=false
&f=json
```

**Example:**
```
https://hazards.fema.gov/gis/nfhl/rest/services/public/NFHL/MapServer/28/query?where=1=1&geometry=-82.4572,27.9506&geometryType=esriGeometryPoint&inSR=4326&spatialRel=esriSpatialRelWithin&outFields=FLD_ZONE&returnGeometry=false&f=json
```

**Flood Zone Types:**
| Zone | Risk Level | Description |
|------|------------|-------------|
| **A, AE, AH, AO** | HIGH | 1% annual flood chance (100-year flood) |
| **V, VE** | HIGH | Coastal with wave action |
| **X (shaded)** | MODERATE | 0.2% annual chance (500-year flood) |
| **X (unshaded)** | MINIMAL | Outside flood hazard area |
| **D** | UNDETERMINED | No flood hazard analysis performed |

### Sunbiz (Florida Division of Corporations)

**No public API.** Web scraping required.

**Search URLs:**
- By Name: `https://search.sunbiz.org/Inquiry/CorporationSearch/ByName`
- By Officer: `https://search.sunbiz.org/Inquiry/CorporationSearch/ByOfficerOrRegisteredAgent`
- By Address: `https://search.sunbiz.org/Inquiry/CorporationSearch/ByAddress`

**Use cases:**
1. Verify if property owner has an active LLC/Corp
2. Find other properties owned by same entity
3. Check if foreclosure defendant is a business

### Accela Citizen Access (Permits)

**No API.** Web scraping with address parsing required.

**Portals:**
- City of Tampa: `https://aca-prod.accela.com/TAMPA/Default.aspx`
- Hillsborough County: `https://aca-prod.accela.com/HCFL/Default.aspx`

**Address Parsing:**
```
Input: "3006 W Julia St Unit A"
→ Street Number: 3006
→ Street Direction: W
→ Street Name: Julia
→ Street Type: St
→ Unit: A
```

**Global search** available via `#txtSearchCondition` input field.

---

## Complete Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    PROPERTY IDENTIFICATION                       │
├─────────────────────────────────────────────────────────────────┤
│  Auctions (RealForeclose/RealTaxDeed)                           │
│      ↓                                                          │
│  Get: Case #, Folio, Address                                    │
│      ↓                                                          │
│  HCPA Bulk Data → Get: Legal Description, Owner, Value          │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    ORI DOCUMENT SEARCH                           │
├─────────────────────────────────────────────────────────────────┤
│  1. Extract subdivision name from legal description              │
│     "LOT 9 BLOCK 12 MUNRO AND MC INTOSHS ADD"                   │
│     → Search term: "MUNRO AND MC INTOSH"                        │
│                                                                  │
│  2. Search ORI API with CONTAINS/BEGINS operator                │
│     → Returns ALL documents in that subdivision                  │
│                                                                  │
│  3. Download all potential matching PDFs                        │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    PDF TEXT EXTRACTION                           │
├─────────────────────────────────────────────────────────────────┤
│  1. Convert PDF → Images (PyMuPDF/fitz @ 150 DPI)               │
│  2. Send to Qwen-VL API (10.10.1.5:6969)                        │
│  3. Extract: Names, Dates, Amounts, Legal Descriptions, Folio   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              ⚠️ PROPERTY VERIFICATION (CRITICAL)                │
├─────────────────────────────────────────────────────────────────┤
│  For each extracted document, verify it matches target:          │
│                                                                  │
│  CHECK 1: Folio Match                                           │
│     Extract "Parcel ID: 191887-0000" from PDF                   │
│     Compare to known folio from HCPA                            │
│                                                                  │
│  CHECK 2: Lot/Block Match                                       │
│     Parse legal: "L 9 B 12" → Lot 9, Block 12                   │
│     Compare to target property's lot/block                       │
│                                                                  │
│  CHECK 3: Address Match                                         │
│     Extract "205 W AMELIA AVE" from PDF                         │
│     Normalize and compare to known address                       │
│                                                                  │
│  CHECK 4: Owner Chain                                           │
│     PartiesOne/PartiesTwo should include known owners           │
│                                                                  │
│  RESULT: Only verified documents proceed to analysis            │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    FINAL ANALYSIS                                │
├─────────────────────────────────────────────────────────────────┤
│  HOVER → Final Judgment PDF (exact debt amounts)                 │
│  Market Data → ARV estimate (Zillow/Realtor)                    │
│  Tax + Permits → Risk flags (unpaid taxes, violations)          │
│      ↓                                                          │
│  Net Equity = Market Value - Verified Liens                     │
└─────────────────────────────────────────────────────────────────┘
```
