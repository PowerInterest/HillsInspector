# Legal Description Direct Search (CQID=321)

**Endpoint:** `https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=321`

This endpoint allows for direct searching of the Official Records Index (ORI) using a property's **Legal Description**. This is a powerful fallback method when Name Search (ORI) or Book/Page Search (HCPA) fails.

## URL Parameters

| Parameter | Description | HTML ID | Notes |
| :--- | :--- | :--- | :--- |
| `CQID` | **Search Type ID** | N/A | Must be `321` for Legal Description Search. |
| `OBKey__1011_1` | **Legal Description** | `obpa_kw_1011` | The main search term. Supports wildcards (`*`). |
| `OBKey__1285_1` | **Document Type** | `obpa_kw_1285` | Filter by type (e.g., `DEED`, `MORTGAGE`). |
| `OBKey__1634_1` | **Recording Date** | `obpa_kw_1634` | Specific recording date. |
| `OBKey__date_from` | **From Date** | `obpa_date_from` | Start of date range. |
| `OBKey__date_to` | **To Date** | `obpa_date_to` | End of date range. |

## Data Returned Per Row

Each row in the results table contains:
- `ORI - Person Type` - PARTY 1 (grantor/from) or PARTY 2 (grantee/to)
- `Name` - Party name
- `Recording Date Time` - Full timestamp
- `ORI - Doc Type` - Document type code (e.g., `(D) DEED`, `(MTG) MORTGAGE`)
- `Book Type` - Usually "O" for Official Records
- `Book #` - Book number
- `Page #` - Page number
- `Legal Description` - Legal description as recorded on this document
- `Instrument #` - Unique document identifier

**Important:** Multiple rows may share the same Instrument # because each party on a document gets their own row. Group by Instrument # to get unique documents.

## Legal Description Variations - Critical Finding

**The same property can have multiple different legal description formats in the Clerk's database.** This is due to inconsistent data entry over time by different title companies, attorneys, and clerks.

### Real Example: LOT 198 TUSCANY SUBDIVISION AT TAMPA PALMS

A search for `L 198 TUSCANY*` returned 32 rows representing 12 unique documents, but with **4 different legal description variations**:

| Variation | Example Documents |
|-----------|-------------------|
| `L 198 TUSCANY AT TAMPA PALMS` | 2010 Deed, 2010 Mortgage |
| `L 198 TUSCANY SUB AT TAMPA PALMS` | 2011 NOC |
| `L 198 TUSCANY SUBD AT TAMPA PALMS` | 2015 Deed, 2018 Deed, 2018 Mortgage |
| `L 198 TUSCANY SUBD TAMPA PALMS` | 2010 Partial Release, 2019 Lien |

**Key Observations:**
- `SUB` vs `SUBD` vs no abbreviation for "SUBDIVISION"
- `AT` sometimes present, sometimes omitted
- `LOT` abbreviated to `L` consistently
- Same property, same chain of title, 4 different formats

### Why This Matters

1. **Searching for one variation may miss documents with other variations**
2. **All variations should be stored in the database** to enable future searches
3. **The wildcard `*` helps catch variations** but may not catch all cases

## Search Strategy & Findings

### 1. Exact Matches Often Fail
Searching for the full, exact legal description string from the Property Appraiser (HCPA) often returns **0 results**.
*   **Reason:** The Clerk's database often abbreviates or formats legal descriptions differently than the Property Appraiser.
*   **Example:** `RIVER BEND PHASE 3A AND 3B LOT 9 BLOCK 12` (HCPA) vs `RIVER BEND PH 3A...` (Clerk).

### 2. Prefix Wildcards Work (`TERM*`)
Using a prefix wildcard is the most effective strategy.
*   **Pattern:** `LOT_ABBREV LOT_NUMBER SUBDIVISION_NAME*`
*   **Example:** `L 198 TUSCANY*`
*   **Result:** Successfully returns all documents for that lot (32 rows / 12 documents in testing).

### 3. Leading Wildcards Cause Timeouts (`*TERM`)
Avoid using leading wildcards.
*   **Pattern:** `*RIVER BEND*`
*   **Result:** Causes the search to hang and eventually **timeout**. This likely forces a full table scan on the database.

### 4. Result Truncation
The server appears to truncate results (around 100 rows) without providing standard pagination controls in the HTML.
*   **Impact:** If a subdivision has hundreds of lots (e.g., "RIVER BEND"), searching for `RIVER BEND*` will only return the first ~100 matches. If your target lot is not in that set, it will not be found.
*   **Workaround:** Be as specific as possible - include the lot number (e.g., `L 198 TUSCANY*` instead of `TUSCANY*`).

## Comprehensive Document Search Strategy

**A single search method is NOT sufficient to find all documents for a property.** You must use multiple search methods and de-duplicate by Instrument #.

### Multi-Method Search Approach

| Priority | Method | CQID | Best For | Limitations |
|----------|--------|------|----------|-------------|
| 1 | **Book/Page Search** | 319 | Deeds, Mortgages from HCPA Sales History | Only finds documents with known Book/Page |
| 2 | **Legal Description Search** | 321 | All document types tied to the property | May miss variations; truncation issues |
| 3 | **Name Search** | 326 | Liens, Judgments, documents tied to a person | May find unrelated documents; need to filter by legal description |

### Recommended Search Flow

1. **Start with HCPA Parcel Page** (`https://gis.hcpafl.org/PropertySearch/#/parcel/basic/{PIN}`)
   - Get legal description (Legal Lines section)
   - Get sales history with Book/Page links
   - Get owner names

2. **Search by Book/Page** (CQID=319) for each sale in history
   - Captures the actual deed/mortgage documents
   - Gets exact legal description as recorded

3. **Search by Legal Description** (CQID=321)
   - Use `LOT_ABBREV LOT# SUBDIVISION*` format
   - Example: `L 198 TUSCANY*`
   - Captures NOCs, Liens, Releases not in sales history

4. **Search by Owner Names** (CQID=326)
   - Search each owner found in the chain
   - Filter results to only those matching the property's legal description
   - Captures personal liens/judgments that may affect title

5. **De-duplicate by Instrument #**
   - Combine all results
   - Group by Instrument # to get unique documents
   - Store all legal description variations found

### Building the Search Term

**From HCPA Legal Description:**
```
raw_legal1: "TUSCANY SUBDIVISION AT TAMPA PALMS"
raw_legal2: "LOT 198"
```

**Convert to ORI Search Term:**
```
1. Replace "LOT " with "L "  →  "L 198"
2. Extract first word of subdivision  →  "TUSCANY"
3. Combine with wildcard  →  "L 198 TUSCANY*"
```

## Example URLs

**Legal Description Search:**
```
https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=321&OBKey__1011_1=L%20198%20TUSCANY%2A
```

**Book/Page Search:**
```
https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=319&OBKey__1530_1=O&OBKey__573_1=26260&OBKey__1049_1=89
```

**Name Search:**
```
https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=326&OBKey__486_1=ROLLISON%20DANA
```

## Database Storage

All found documents should be saved to the `documents` table with:
- `folio` - Property identifier (links all documents to the property)
- `instrument_number` - Unique document ID
- `document_type` - DEED, MORTGAGE, LIEN, NOC, etc.
- `recording_date` - When recorded
- `book`, `page` - Official Records location
- `party1`, `party2` - All parties (semicolon-separated if multiple)
- `legal_description` - **Store the exact legal description from THIS document** (preserves variations)

By storing the legal description from each document, we automatically capture all variations and can use them for future searches.

## Chain of Title Analysis

Once documents are collected, they can be analyzed to build:

1. **Ownership Timeline** - Sort deeds by date to trace: Builder → Owner1 → Owner2 → Current
2. **Mortgage History** - Track loans and identify if satisfied or still active
3. **Liens & Encumbrances** - HOA liens, mechanic's liens, tax liens
4. **NOCs** - Construction permits and contractor involvement
5. **Releases/Satisfactions** - Match to original liens to determine what's cleared

### Document Types for Analysis

| Code | Type | Significance |
|------|------|--------------|
| `(D) DEED` | Deed | Ownership transfer |
| `(MTG) MORTGAGE` | Mortgage | Loan against property |
| `(LN) LIEN` | Lien | Claim against property |
| `(NOC) NOTICE OF COMMENCEMENT` | NOC | Construction work started |
| `(PR) PARTIAL RELEASE` | Partial Release | Part of lien/mortgage released |
| `(SAT) SATISFACTION` | Satisfaction | Lien/mortgage paid off |
| `(RELLP) RELEASE LIS PENDENS` | Release Lis Pendens | Lawsuit notice released |
| `(AFF) AFFIDAVIT` | Affidavit | Sworn statement (often title affidavit at closing) |
| `(LP) LIS PENDENS` | Lis Pendens | Notice of pending lawsuit |
