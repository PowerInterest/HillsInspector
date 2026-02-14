# HillsInspector API Endpoints & URLs

## External APIs (County Services & Third-Party)

### Hillsborough County Clerk of Court - ORI (Official Records Index)

| Method | URL Pattern | Description | Source |
|--------|-------------|-------------|--------|
| GET | `https://publicaccess.hillsclerk.com/oripublicaccess/` | ORI Public Access main portal (requires session init for CORS) | `ori_api_scraper.py` |
| POST | `https://publicaccess.hillsclerk.com/Public/ORIUtilities/DocumentSearch/api/Search` | ORI case/party search API (accepts JSON: CaseNum, PartyName) | `ori_api_scraper.py` |
| GET | `https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html` | PAV Direct Search base URL | `ori_scraper.py` |
| GET | `https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=319&OBKey__1530_1={book_type}&OBKey__573_1={book}&OBKey__1049_1={page}` | Book/Page search (CQID 319) | `ori_scraper.py` |
| GET | `https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=320&OBKey__1006_1={instrument}` | Instrument number search (CQID 320) | `ori_scraper.py` |
| GET | `https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=321&OBKey__1011_1={legal_desc}` | Legal description search (CQID 321) | `ori_scraper.py`, `ori_api_scraper.py` |
| GET | `https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=326&OBKey__486_1={name}` | Party name search (CQID 326) | `ori_scraper.py`, `ori_api_scraper.py` |
| POST | `https://publicaccess.hillsclerk.com/PAVDirectSearch/api/CustomQuery/KeywordSearch` | PAV Direct Search API (returns JSON with document data) | `ori_api_scraper.py` |
| GET | `https://publicaccess.hillsclerk.com/PAVDirectSearch/api/Document/{encoded_id}/?OverlayMode=View` | PDF download endpoint (requires URL-encoded doc ID) | `auction_scraper.py`, `ori_api_scraper.py` |
| POST | `https://publicaccess.hillsclerk.com/Public/ORIUtilities/OverlayWatermark/api/Watermark` | Alternative PDF watermark endpoint | `ori_api_scraper.py` |

### Hillsborough County Clerk - HOVER System

| Method | URL Pattern | Description | Source |
|--------|-------------|-------------|--------|
| GET | `https://hover.hillsclerk.com` | Court case document system (PerimeterX protected) | `hover_scraper.py` |

### Hillsborough County Property Appraiser (HCPA)

| Method | URL Pattern | Description | Source |
|--------|-------------|-------------|--------|
| GET | `https://gis.hcpafl.org/propertysearch/` | HCPA GIS main portal | `hcpa_scraper.py` |
| GET | `https://gis.hcpafl.org/propertysearch/#/parcel/basic/{parcel_id}` | Property details page by parcel ID | `hcpa_gis_scraper.py` |
| GET | `https://gis.hcpafl.org/propertysearch/#/search/basic` | Property search page (search by folio) | `hcpa_gis_scraper.py` |
| GET | `https://www.hcpafl.org/Downloads/GIS` | Bulk parcel data downloads (DBF/Parquet) | bulk enrichment |

### Foreclosure & Tax Deed Auctions

| Method | URL Pattern | Description | Source |
|--------|-------------|-------------|--------|
| GET | `https://hillsborough.realforeclose.com` | Foreclosure auction calendar and listings | `auction_scraper.py` |
| GET | `https://hillsborough.realforeclose.com/index.cfm?zaction=user&zmethod=calendar` | Auction calendar view | `auction_scraper.py` |
| GET | `https://hillsborough.realforeclose.com/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE={date}` | Auction preview for specific date | `auction_scraper.py` |
| GET | `https://hillsborough.realtaxdeed.com` | Tax deed auction portal | `tax_deed_scraper.py` |

### Building Permits - Accela Platform

| Method | URL Pattern | Description | Source |
|--------|-------------|-------------|--------|
| GET | `https://aca-prod.accela.com/TAMPA/Default.aspx` | City of Tampa permit portal | `permit_scraper.py` |
| GET | `https://aca-prod.accela.com/HCFL/Default.aspx` | Hillsborough County permit portal | `permit_scraper.py` |
| GET | `https://aca-prod.accela.com/TAMPA/Cap/GlobalSearchResults.aspx?isNewQuery=yes&QueryText={address}#CAPList` | Tampa global search by address | `permit_scraper.py` |

### Tax Payment Status

| Method | URL Pattern | Description | Source |
|--------|-------------|-------------|--------|
| GET | `https://county-taxes.net/hillsborough/property-tax` | Tax payment portal (Cloudflare-free alternative) | `tax_scraper.py` |
| GET | `https://hillsborough.county-taxes.com/public` | Official tax portal | `tax_scraper.py` |
| GET | `https://lienhub.com/county/hillsborough` | Lien information portal | `tax_scraper.py` |

### Florida Business Registry

| Method | URL Pattern | Description | Source |
|--------|-------------|-------------|--------|
| GET | `https://search.sunbiz.org` | FL Dept of State business entity search | `sunbiz_scraper.py` |

### FEMA Flood Zones

| Method | URL Pattern | Description | Source |
|--------|-------------|-------------|--------|
| GET | `https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer` | FEMA National Flood Hazard Layer (ArcGIS REST) | `fema_flood_scraper.py` |

### Real Estate Listing Data

| Method | URL Pattern | Description | Source |
|--------|-------------|-------------|--------|
| GET | `https://www.zillow.com/homes/{address-city-state-zip}_rb/` | Zillow property search | `market_scraper.py` |
| GET | `https://www.realtor.com/realestateandhomes-search/{city}_{state}/{address}` | Realtor.com property search | `market_scraper.py` |
| POST | `https://realtor-data1.p.rapidapi.com/property_list/` | Realtor.com RapidAPI endpoint | `realtor_api.py` |
| GET | `https://www.redfin.com/stingray` | Redfin Stingray API base | `redfin_scraper.py` |
| GET | `https://www.auction.com` | Auction.com real estate auctions | `auction_com_scraper.py` |

### Geocoding

| Method | URL Pattern | Description | Source |
|--------|-------------|-------------|--------|
| GET | `https://nominatim.openstreetmap.org/search?format=json&limit=1&q={address}` | OpenStreetMap Nominatim geocoder | `geocoder.py` |

### Vision/OCR Services

| Method | URL Pattern | Description | Source |
|--------|-------------|-------------|--------|
| POST | `http://192.168.86.26:6969/v1/chat/completions` | Primary local vision (GLM-4.6v-flash) | `vision_service.py` |
| POST | `http://10.10.0.33:6969/v1/chat/completions` | Secondary local vision (Qwen3-VL-8B) | `vision_service.py` |
| POST | `http://10.10.1.5:6969/v1/chat/completions` | Tertiary local vision (Qwen3-VL-8B) | `vision_service.py` |
| POST | `http://10.10.2.27:6969/v1/chat/completions` | Quaternary local vision (Qwen3-VL-8B) | `vision_service.py` |
| POST | `https://api.openai.com/v1/chat/completions` | OpenAI cloud fallback (gpt-4o) | `vision_service.py` |
| POST | `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent` | Google Gemini cloud fallback | `vision_service.py` |

---

## Internal Web Routes (FastAPI Dashboard)

### Dashboard Routes (`app/web/routers/dashboard.py`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Main dashboard homepage |
| GET | `/auctions` | Auctions list view |
| GET | `/auctions/{auction_date}` | Auctions by specific date |

### Property Routes (`app/web/routers/properties.py`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/property/{folio}` | Property detail page |
| GET | `/property/{folio}/liens` | Property liens view |
| GET | `/property/{folio}/documents` | Property documents |
| GET | `/property/{folio}/analysis` | Lien survival analysis |
| GET | `/property/{folio}/sales` | Sales history |
| GET | `/property/{folio}/market` | Market data |
| GET | `/property/{folio}/tax` | Tax payment info |
| GET | `/property/{folio}/permits` | Building permits |
| GET | `/property/{folio}/chain` | Chain of title |
| GET | `/property/{folio}/judgment` | Final judgment details |
| GET | `/property/{folio}/doc/{doc_id}` | Download document file |
| GET | `/property/{folio}/title-report` | Title report view |

### API Routes (`app/web/routers/api.py`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/map-auctions` | GeoJSON data for auction map |
| GET | `/api/health` | API health check |

### Review Routes (`app/web/routers/review.py`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/review/hcpa-failures` | HCPA scraper failures review |
| POST | `/review/hcpa-failures/{case_number}/mark-reviewed` | Mark case as reviewed |

### History Routes (`app/web/routers/history.py`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/history` | Historical auction data page |
| GET | `/history/data` | Historical data JSON endpoint |

### Global Routes (`app/web/main.py`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Application health check |

---

## ORI Search Parameters

### Browser-Based Searches (CQID URLs)

The PAV Direct Search Angular SPA accepts search parameters via URL query strings.
Each CQID corresponds to a different search type. The SPA parses `OBKey__*` params
and automatically POSTs to the `CustomQuery/KeywordSearch` API endpoint.

**Book/Page Search (CQID 319)**:
- `OBKey__1530_1` = Book type (`OR` for Official Records, `P` for Plat)
- `OBKey__573_1` = Book number
- `OBKey__1049_1` = Page number

**Instrument Search (CQID 320)**:
- `OBKey__1006_1` = Instrument number

**Legal Description Search (CQID 321)**:
- `OBKey__1011_1` = Legal description text (URL encoded, supports `*` wildcard)

**Party Name Search (CQID 326)**:
- `OBKey__486_1` = Party name (URL encoded)

### Direct API: CustomQuery/KeywordSearch

The PAV Direct Search API can be called directly (no browser needed) via POST to:
```
POST https://publicaccess.hillsclerk.com/PAVDirectSearch/api/CustomQuery/KeywordSearch
```

**Required Headers:**
```json
{
  "Content-Type": "application/json",
  "Origin": "https://publicaccess.hillsclerk.com",
  "Referer": "https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=..."
}
```

**Request Payload:**
```json
{
  "QueryID": <CQID>,
  "Keywords": [
    {"Id": <keyword_id>, "Value": "<search_value>"}
  ]
}
```

**Keyword IDs (mapped from OBKey URL params):**

| CQID | Search Type | Keyword Id | OBKey Param | Example Value |
|------|-------------|-----------|-------------|---------------|
| 319 | Book/Page | 573 | `OBKey__573_1` | `"24546"` (book number) |
| 319 | Book/Page | 1049 | `OBKey__1049_1` | `"1828"` (page number) |
| 319 | Book/Page | 1530 | `OBKey__1530_1` | `"OR"` (book type) |
| 320 | Instrument | 1006 | `OBKey__1006_1` | `"2024478600"` |
| 321 | Legal Desc | 1011 | `OBKey__1011_1` | `"L 198 TUSCANY*"` |
| 326 | Party Name | 486 | `OBKey__486_1` | `"SMITH JOHN"` |

**Response Format:**
```json
{
  "Data": [
    {
      "ID": "<encoded_doc_id>",
      "Name": "<HTML-formatted summary>",
      "DisplayType": "Image",
      "DisplayColumnValues": [
        {"Value": "PARTY 1", "RawValue": null},
        {"Value": "SMITH JOHN", "RawValue": null},
        {"Value": "11/23/2016 12:15:28 PM", "RawValue": "1479903328000"},
        {"Value": "(D) DEED", "RawValue": null},
        {"Value": "O", "RawValue": null},
        {"Value": "24546", "RawValue": null},
        {"Value": "1828", "RawValue": "1828"},
        {"Value": "PT L 1 B 22 BAYBRIDGE SUBD REV", "RawValue": null},
        {"Value": "2016461130", "RawValue": "2016461130"}
      ]
    }
  ]
}
```

**DisplayColumnValues order:** `[person_type, name, record_date, doc_type, book_type, book_num, page_num, legal, instrument]`

**Notes:**
- Zero results = `{"Data": []}` returned instantly
- Each document party gets its own record (PARTY 1, PARTY 2 etc.)
- The `ID` field is the encoded document ID for PDF download via `/PAVDirectSearch/api/Document/{ID}/?OverlayMode=View`
- The ORIUtilities Search API (`/Public/ORIUtilities/DocumentSearch/api/Search`) does NOT support instrument search (returns 400: "cannot convert to Int64")
- Keyword field must use `"Id"` (capitalized) — `"Name"` accepts string keyword names but `"KeywordID"` returns 400

### ORI Search API (ORIUtilities)

Separate from the PAV Direct Search. Used for case number and party name searches.

```
POST https://publicaccess.hillsclerk.com/Public/ORIUtilities/DocumentSearch/api/Search
```

**Supported fields:** `CaseNum`, `Party` (alias: `PartyName`), `DocType`, `RecordDateBegin`, `RecordDateEnd`

**NOT supported:** `Instrument` (returns 400: "JSON value could not be converted to System.Int64")

**Requires session:** Must first navigate to `https://publicaccess.hillsclerk.com/oripublicaccess/` to establish CORS session. If the site is down/slow, this navigation times out (30s).

## Case Number Formats

- `29YYYYCA######` = Circuit Court (Civil) - Mortgage foreclosures
- `29YYYYCC######` = County Court - HOA liens, code enforcement, small claims
