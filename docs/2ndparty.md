# Party Extraction and Cross-Party Resolution

This document covers the challenges of extracting both parties (grantor/grantee) from ORI documents, handling self-transfers, and strategies for resolving missing party data.

## Table of Contents

1. [The Problem](#the-problem)
2. [Why Party 2 Goes Missing](#why-party-2-goes-missing)
3. [ORI Search Endpoints](#ori-search-endpoints)
4. [Self-Transfer Deeds](#self-transfer-deeds)
5. [Resolution Strategies](#resolution-strategies)
6. [Implementation Plan](#implementation-plan)

---

## The Problem

When building a chain of title, we need both parties for each deed:
- **Party 1 (Grantor)**: The person/entity transferring ownership
- **Party 2 (Grantee)**: The person/entity receiving ownership

However, our ORI searches frequently return documents with only Party 1 indexed. Without Party 2, we cannot:
1. Determine who acquired the property
2. Link the deed to the next ownership period
3. Build a complete chain of title

### Example: Instrument 2024478600

```
Search: RETREAT ON DAVIS ISLAND*
API Response:
{
  "Instrument": "2024478600",
  "DocType": "(D) DEED",
  "PartiesOne": ["BARGAMIN KRISTEN H"],
  "PartiesTwo": []  // Empty - no grantee!
}
```

This deed appears in our database with a grantor but no grantee, breaking the chain.

---

## Why Party 2 Goes Missing

### 1. Split Indexing by Legal Description

ORI indexes parties against the legal description text that appears near their name on the document. If the grantor and grantee sections have slightly different legal description text, they get indexed separately:

```
Document Page 1 (Grantor section):
  "KRISTEN H. BARGAMIN, grantor, of the property described as:
   RETREAT ON DAVIS ISLAND UNIT 202..."

Document Page 2 (Grantee section):
  "...to JOHN DOE, grantee, being
   Unit 202, The Retreat on Davis Island Condominium..."
```

Our search for `RETREAT ON DAVIS ISLAND*` finds Party 1 but not Party 2 because Party 2's legal text says "The Retreat on Davis Island Condominium" (different format).

### 2. Incomplete Data Entry

The Clerk's office may not have finished indexing all parties, especially for:
- Recent recordings (within 30-60 days)
- High-volume recording days
- Complex documents with many parties

### 3. Historical Records

Pre-1990 documents often have incomplete party indexing. Sometimes only the grantor was indexed.

### 4. Party Name Variations

Party 2 may be indexed under a different name format:
- Grantor indexed as: "SMITH JOHN A"
- Grantee indexed as: "SMITH JOHN ADAM" or "JOHN A SMITH"

### 5. Self-Transfer Deeds

Sometimes grantor and grantee are THE SAME PERSON. This is legitimate and occurs when:
- Changing vesting (individual → trust)
- Adding/removing spouse from title
- Correcting a previous deed
- Estate planning transfers

---

## ORI Search Endpoints

ORI has multiple search interfaces with different CQIDs:

### Currently Used

**CQID 321 - Legal Description Search**
```
URL: https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=321&OBKey__1011_1={legal}
```
- Searches by legal description text
- Returns one row per party per document
- Requires grouping by instrument number

### CQID 326 - Party Name Search (KEY DISCOVERY)

**URL Format:**
```
https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=326&OBKey__486_1={party_name}*
```

**This is the most useful endpoint for cross-party resolution!**

Unlike the legal description search (CQID 321), the party name search returns ALL documents where a person appears as EITHER Party 1 OR Party 2.

**Example - Searching for BARGAMIN KRISTEN*:**
```
Found 45 rows:
PARTY 1 | BARGAMIN KRISTEN H | (D) DEED | 11/19/2024 | 2024478599
PARTY 2 | BARGAMIN KRISTEN H | (D) DEED | 11/19/2024 | 2024478599  <- Both parties same doc!
PARTY 1 | BARGAMIN KRISTEN H | (D) DEED | 11/19/2024 | 2024478600  <- Only Party 1 indexed
PARTY 2 | BARGAMIN KRISTEN H | (D) DEED | 11/19/2024 | 2024478601
...
```

**Key Insight**: For instrument 2024478600, even the party name search shows NO Party 2 entry. This confirms the data simply doesn't exist in ORI - it's not a search methodology issue.

### Known OBKey Parameters

The PAVDirectSearch interface uses OBKey parameters to pass search criteria. These can be **combined** across different CQIDs to create targeted searches:

| OBKey Parameter | Purpose | CQID Used With |
|-----------------|---------|----------------|
| `OBKey__486_1` | Party name | 318, 326 |
| `OBKey__1006_1` | Instrument number | 320 |
| `OBKey__1011_1` | Legal description text | 319, 321, 322 |
| `OBKey__573_1` | Book number | 319 |
| `OBKey__1049_1` | Page number | 319 |
| `OBKey__1530_1` | Book type flag (e.g., "O" for Official) | 319 |

**Combined Search Example:**

To search for a specific party on a specific instrument:
```
CQID=326&OBKey__486_1=SMITH*&OBKey__1006_1=2024478600
```

This filters the party search to a single instrument, making it much faster than searching all documents for a party name.

### Other CQIDs Tested

| CQID | Parameters | Purpose | Useful? |
|------|-----------|---------|---------|
| 319 | Book/Page/Type | Book/Page Search | Yes |
| 320 | Instrument | Instrument Number Search | Yes |
| 321 | Legal | Legal Description Search | Current method |
| 322 | Legal | Unknown - same as 321 | No |
| **326** | **Party Name** | **Party Name Search** | **YES - Cross-party!** |
| 349 | Legal | Pending/Recent Recordings | Limited use |

### Cross-Party Resolution Strategy

Using CQID 326, we can attempt to find missing Party 2:

1. **Search by Grantor Name**: If we have Party 1 but not Party 2, search for Party 1's name
2. **Filter by Instrument**: Find all rows for the target instrument number
3. **Check for Party 2**: If Party 2 exists for that instrument, we found it
4. **If Still Missing**: The data genuinely doesn't exist in ORI - use vLLM OCR

```python
async def find_party2_via_party_search(grantor_name: str, instrument: str) -> str | None:
    """Search CQID 326 to find Party 2 for an instrument."""
    url = f"...CQID=326&OBKey__486_1={quote(grantor_name + '*')}"
    results = await search_ori(url)

    for row in results:
        if row['instrument'] == instrument and 'PARTY 2' in row['person_type']:
            return row['name']

    return None  # Party 2 not indexed - need OCR
```

### API Method

The JSON API at `/Public/ORIUtilities/DocumentSearch/api/Search` returns both parties as arrays:

```json
{
  "Instrument": "2014318870",
  "PartiesOne": ["STANDARD PACIFIC OF FLORIDA"],
  "PartiesTwo": ["MAIDA AMANDA", "MAIDA CHRISTOPHER S"]
}
```

This is cleaner than browser scraping but still has the same underlying data - if Party 2 isn't indexed, it won't appear.

---

## Self-Transfer Deeds

### What They Are

A self-transfer deed is when the grantor and grantee are the same person or entity. These are common and legitimate:

```
GRANTOR: KRISTEN H. BARGAMIN
GRANTEE: KRISTEN H. BARGAMIN, as Trustee of the Bargamin Family Trust
```

### Why They Exist

1. **Trust Transfers**: Moving property from individual ownership into a trust for estate planning
2. **Vesting Changes**: Changing how title is held (joint tenants → tenants in common)
3. **Name Changes**: Updating records after marriage/divorce/legal name change
4. **Corrective Deeds**: Fixing errors in previous deeds
5. **Entity Restructuring**: Moving property between related entities

### How ORI Handles Them

ORI often only indexes Party 1 for self-transfers because:
- Data entry staff may recognize it's the same person and skip Party 2
- The indexing system may detect duplicate names and omit one
- Some self-transfers use simplified forms without separate grantee sections

### Impact on Chain of Title

Self-transfer deeds **do not represent an ownership change**. For chain building:
- They should be noted but not counted as transfers
- The ownership period continues unchanged
- They may affect HOW title is held (e.g., trust) but not WHO owns it

### Detection Strategies

1. **Name Matching**: Compare Party 1 and Party 2 names (fuzzy match for variations)
2. **Document Type**: Certain deed types are commonly self-transfers:
   - Quitclaim deeds between family members
   - Trust deeds
   - Corrective deeds
3. **Consideration Amount**: Self-transfers often show $10 or $0 consideration
4. **OCR Extraction**: Use vLLM to read the actual document and compare parties

### Handling in Database

When a self-transfer is detected:
```python
{
    "instrument": "2024478600",
    "party1": "KRISTEN H. BARGAMIN",
    "party2": "KRISTEN H. BARGAMIN",
    "is_self_transfer": True,
    "self_transfer_type": "vesting_change",  # or trust, name_change, corrective
    "affects_chain": False  # Does not represent ownership change
}
```

---

## Resolution Strategies

### Strategy 1: Multiple Legal Description Searches

Search with variations of the legal description to find Party 2 under different indexing:

```python
search_terms = [
    "RETREAT ON DAVIS ISLAND*",
    "UNIT 202 RETREAT*",
    "RETREAT*CONDO*",
    "202 RETREAT ON DAVIS*",
]

all_results = []
for term in search_terms:
    results = search_ori(term)
    all_results.extend(results)

# Group by instrument and merge parties
merged = group_by_instrument(all_results)
```

### Strategy 2: API Search with Full Document Types

Use the API to search with all document types to catch related documents:

```python
payload = {
    "DocType": ["(D) DEED", "(QC) QUIT CLAIM", "(WD) WARRANTY DEED", ...],
    "Legal": ["CONTAINS", "RETREAT ON DAVIS"],
}
```

### Strategy 3: Instrument Number Lookup

Once we have an instrument number, try to look it up directly:

```
CQID=319&OBKey__1011_1=2024478600
```

This might return all parties indexed against that instrument.

### Strategy 4: vLLM Document OCR

Download the PDF and use vision AI to extract parties:

```python
from src.services.vision_service import VisionService
from src.scrapers.ori_api_scraper import ORIApiScraper

# Download PDF
scraper = ORIApiScraper()
pdf_path = scraper.download_pdf(doc, output_dir)

# Convert to image
image_path = convert_pdf_to_image(pdf_path)

# Extract with vLLM
vision = VisionService()
result = vision.extract_deed(image_path)

grantee = result.get("grantee")
```

### Strategy 5: Cross-Reference with HCPA

The Property Appraiser (HCPA) has the current owner. For recent deeds:
1. Get current owner from HCPA
2. If our deed has a grantor but no grantee
3. And the recording date is recent
4. The HCPA owner might be the missing grantee

### Strategy 6: Explore CQIDs 324-348

**TODO**: Systematically test each CQID to understand what data they return:

```python
for cqid in range(324, 349):
    url = f"https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID={cqid}&OBKey__1011_1=TEST"
    # Document what each returns
```

---

## Implementation Plan

### Phase 1: Detect Missing Party 2

In the ingestion pipeline, flag documents with missing Party 2:

```python
def flag_missing_party2(documents):
    """Flag documents that need Party 2 resolution."""
    for doc in documents:
        if doc.get("document_type") in DEED_TYPES:
            if not doc.get("party2"):
                doc["needs_party2_resolution"] = True
                doc["resolution_attempted"] = False
```

### Phase 2: Automatic Resolution Service

Create a service that attempts to resolve missing Party 2:

```python
class Party2ResolutionService:
    def __init__(self):
        self.ori_scraper = ORIApiScraper()
        self.vision_service = VisionService()

    async def resolve_party2(self, doc: dict) -> dict:
        """Attempt to find missing Party 2."""

        # Strategy 1: Try alternative legal description searches
        party2 = await self._try_alternative_searches(doc)
        if party2:
            return {"party2": party2, "method": "alternative_search"}

        # Strategy 2: Try instrument lookup
        party2 = await self._try_instrument_lookup(doc)
        if party2:
            return {"party2": party2, "method": "instrument_lookup"}

        # Strategy 3: Download PDF and OCR
        party2, is_self_transfer = await self._try_ocr_extraction(doc)
        if party2:
            return {
                "party2": party2,
                "method": "ocr_extraction",
                "is_self_transfer": is_self_transfer
            }

        return {"party2": None, "method": "unresolved"}
```

### Phase 3: Self-Transfer Detection

When Party 2 is extracted, check if it's a self-transfer:

```python
def detect_self_transfer(party1: str, party2: str) -> tuple[bool, str]:
    """Detect if this is a self-transfer deed."""

    # Normalize names
    p1_normalized = normalize_name(party1)
    p2_normalized = normalize_name(party2)

    # Check for exact match
    if p1_normalized == p2_normalized:
        return True, "exact_match"

    # Check for trust transfers (same base name + "trustee")
    if "trustee" in p2_normalized.lower():
        base_name = extract_base_name(p2_normalized)
        if names_match(p1_normalized, base_name):
            return True, "trust_transfer"

    # Check for fuzzy match (name variations)
    similarity = calculate_name_similarity(p1_normalized, p2_normalized)
    if similarity > 0.85:
        return True, "name_variation"

    return False, None
```

### Phase 4: Database Updates

Store resolution results:

```sql
ALTER TABLE documents ADD COLUMN party2_resolution_method VARCHAR;
ALTER TABLE documents ADD COLUMN is_self_transfer BOOLEAN DEFAULT FALSE;
ALTER TABLE documents ADD COLUMN self_transfer_type VARCHAR;
ALTER TABLE documents ADD COLUMN party2_extracted_by_ocr BOOLEAN DEFAULT FALSE;
```

### Phase 5: Chain Building Integration

Update chain builder to handle self-transfers:

```python
def build_deed_chain(deeds):
    chain = []
    for deed in sorted(deeds, key=lambda x: x["recording_date"]):
        if deed.get("is_self_transfer"):
            # Note the self-transfer but don't create new ownership period
            if chain:
                chain[-1]["self_transfers"].append(deed)
            continue

        # Normal deed - create new ownership period
        chain.append({
            "owner": deed["party2"],
            "acquired_from": deed["party1"],
            "acquisition_date": deed["recording_date"],
            "self_transfers": []
        })
    return chain
```

---

## Testing

### Test Cases

1. **Normal Deed**: Party 1 and Party 2 are different people
2. **Self-Transfer (Same Name)**: Grantor = Grantee exactly
3. **Self-Transfer (Trust)**: "JOHN SMITH" → "JOHN SMITH, Trustee"
4. **Self-Transfer (Name Variation)**: "JOHN A SMITH" → "JOHN ADAM SMITH"
5. **Missing Party 2 (ORI Issue)**: Party 2 exists on document but not in ORI
6. **Missing Party 2 (Data Entry)**: Recent recording not yet indexed

### Validation

For each resolved Party 2:
1. Log the resolution method
2. Flag low-confidence OCR extractions for manual review
3. Track success rate by method
4. Compare with HCPA owner data when available

---

## Open Questions

1. **What do CQIDs 324-348 return?** - Need to investigate these endpoints
2. **Is there an instrument-based API endpoint?** - Could return all parties for an instrument
3. **How long does ORI take to index new recordings?** - Helps set expectations for recent docs
4. **Should we re-attempt resolution periodically?** - For recently recorded docs that may get indexed later

---

## References

- `src/scrapers/ori_api_scraper.py` - ORI search and PDF download
- `src/services/vision_service.py` - vLLM integration for OCR
- `src/services/ingestion_service.py` - Document ingestion pipeline
- `src/services/title_chain_service.py` - Chain of title building
- `docs/Legal_Descriptions.md` - Legal description parsing details
