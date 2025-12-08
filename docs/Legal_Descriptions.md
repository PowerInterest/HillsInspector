# Legal Description Parsing and ORI Search

This document details how legal descriptions are parsed and used to search the Official Records Index (ORI) for Hillsborough County. Understanding these patterns is critical for building accurate chains of title.

## Table of Contents

1. [Overview](#overview)
2. [Legal Description Structure](#legal-description-structure)
3. [ORI Search Behavior](#ori-search-behavior)
4. [Parsing Challenges](#parsing-challenges)
5. [Search Term Generation](#search-term-generation)
6. [Document Grouping](#document-grouping)
7. [Data Quality Issues](#data-quality-issues)
8. [Known Patterns and Edge Cases](#known-patterns-and-edge-cases)

---

## Overview

Legal descriptions are the official way properties are identified in recorded documents. Unlike addresses (which can change), legal descriptions are tied to plat maps and remain constant. However, they can be written in many different formats:

```
LOT 198 BLOCK 3 TUSCANY SUBDIVISION AT TAMPA PALMS
L 198 B 3 TUSCANY SUBDIVISION
L 198 BLK 3 TUSCANY SUB
LOT 198 TUSCANY SUBDIVISION
```

All of these refer to the same property, but ORI searches are format-sensitive.

---

## Legal Description Structure

### Common Components

| Component | Full Form | Abbreviations | Examples |
|-----------|-----------|---------------|----------|
| Lot | LOT | L, LT | LOT 27, L 5, LOT A5 |
| Block | BLOCK | BLK, B, BK | BLOCK 3, BLK D, B 7 |
| Unit | UNIT | U, UN | UNIT 304, U 5 |
| Phase | PHASE | PH | PHASE 1, PH 2 |
| Section | SECTION | SEC, S | SECTION 110 |
| Subdivision | SUBDIVISION | SUBDIV, SUBD, SUB, S/D | TUSCANY SUBDIVISION |

### Lot and Block Identifiers

Lots and blocks can be:
- **Numeric**: LOT 5, BLOCK 3
- **Alphabetic**: LOT A, BLOCK D
- **Alphanumeric**: LOT 5A, LOT A5, BLOCK 3B

This is critical for parsing - a regex that only matches `\d+` will miss `BLOCK D`.

### Subdivision Name Patterns

Subdivisions often include suffixes:
- **Standard suffixes**: SUBDIVISION, SUB, ESTATES, HEIGHTS, PARK, VILLAGE, MANOR
- **Section numbers**: WESTCHASE SECTION 110, TAMPA PALMS SECTION 23
- **Phase numbers**: LAKE ST CHARLES PHASE 1, CARROLLWOOD PHASE 2
- **Replats**: REVISED MAP OF HOLDENS SUBD, AMENDED PLAT OF OAK PARK

### Section-Township-Range (STR)

Rural properties may use STR notation instead of subdivisions:
```
23-24-25 (Section 23, Township 24, Range 25)
NE 1/4 OF SEC 1-28-19
```

---

## ORI Search Behavior

### How ORI Search Works

The Hillsborough County Official Records Index uses a **CONTAINS** search on the legal description field. Key behaviors:

1. **Wildcard Required**: Searches without wildcards often fail. Always append `*` to search terms.
   - `TUSCANY` - May return 0 results
   - `TUSCANY*` - Returns all documents containing "TUSCANY..."

2. **Case Insensitive**: Searches are case-insensitive.

3. **Word Order Matters**: `LOT 5 TUSCANY*` and `TUSCANY LOT 5*` may return different results.

4. **Partial Matches**: `TUSC*` will match "TUSCANY SUBDIVISION", "TUSCANY ESTATES", etc.

### Search URL Format

```
https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=321&OBKey__1011_1={search_term}
```

The search term is URL-encoded (spaces become `%20`).

### Results Per Party

**CRITICAL**: ORI returns one row per party per document. A single deed with:
- 1 grantor (seller)
- 2 grantees (buyers)

Returns **3 rows** in the search results:
```
Instrument 2024123456 | PARTY 1 | SMITH JOHN (grantor)
Instrument 2024123456 | PARTY 2 | JONES MARY (grantee)
Instrument 2024123456 | PARTY 2 | JONES ROBERT (grantee)
```

These must be **grouped by instrument number** before processing.

---

## Parsing Challenges

### Challenge 1: Letter Blocks

**Problem**: Legal descriptions like `LOT 27 BLOCK D WILLOW BEND SUBDIVISION` have alphabetic block identifiers.

**Failed Pattern**: `\bBLOCK\s+\d+\b` - Only matches numeric blocks

**Correct Pattern**: `\bBLOCK\s+[A-Z]?\d*[A-Z]?\b` - Matches BLOCK D, BLOCK 3, BLOCK 3A

**Impact**: Without this fix, "BLOCK D" was being included in the subdivision name, producing search terms like `BLOCK D WILLOW*` instead of `WILLOW*`.

### Challenge 2: "A SUBDIVISION" False Match

**Problem**: Judgment legal descriptions often include boilerplate text:
```
LOT 9, BLOCK 7, WESTCHASE SECTION 110, A SUBDIVISION ACCORDING TO THE PLAT THEREOF...
```

A naive regex for subdivision names might match "A SUBDIVISION" instead of "WESTCHASE SECTION 110".

**Solution**:
1. Require at least 2 letters before subdivision suffixes: `[A-Z]{2}[A-Z\s]*(?:SUBDIVISION|...)`
2. Add "A SUBDIVISION*" to the filtered generic terms list
3. Extract SECTION/PHASE patterns before removing other identifiers

### Challenge 3: PHASE in Subdivision Names

**Problem**: `LAKE ST CHARLES PHASE 1` - The phase number is part of the subdivision name.

If we remove `PHASE\s+\d+` before extracting the subdivision, we get "LAKE ST CHARLES" instead of "LAKE ST CHARLES PHASE 1".

**Solution**: Extract PHASE patterns on the original text BEFORE removing lot/block identifiers:
```python
early_subdiv_patterns = [
    r'\b([A-Z]{2}[A-Z\s]+PHASE\s+\d+[A-Z]?)\b',
    r'\b([A-Z]{2}[A-Z\s]*\s+SECTION\s+\d+[A-Z]?)\b',
]
```

### Challenge 4: Multi-Word Names with Short Words

**Problem**: `LAKE ST CHARLES` contains "ST" (2 letters), which can trip up patterns that expect longer words.

**Solution**: Use flexible patterns that allow short words within longer phrases:
```python
r'\b([A-Z]{2}[A-Z\s]+PHASE\s+\d+[A-Z]?)\b'  # Requires 2+ letters to start, allows any words after
```

---

## Search Term Generation

### Priority Order

Search terms are generated in order of specificity:

1. **Subdivision wildcard** (most reliable): `TUSCANY*`
2. **Lot + subdivision**: `L 5 TUSCANY*`, `LOT 5 TUSCANY*`
3. **Lot + block + subdivision**: `L 5 B 3 TUSCANY*`
4. **Two-word subdivision**: `TUSCANY SUBDIVISION*`
5. **Section-Township-Range**: `23-24-25*`

### Filtered Generic Terms

Some search terms are too broad and would return thousands of results:

```python
generic_terms = {
    'BLOCK*', 'LOT*', 'UNIT*', 'PHASE*', 'THE*', 'PLAT*', 'BOOK*', 'PAGE*',
    'NORTH*', 'SOUTH*', 'EAST*', 'WEST*', 'SECTION*', 'TOWNSHIP*', 'RANGE*',
    'LESS*', 'THAT*', 'PART*', 'BEING*', 'ALSO*', 'A*', 'AN*', 'AND*',
    'A SUBDIVISION*', 'A SUB*', 'ACCORDING*', 'CORNER*'
}
```

### Specificity Requirements

Search terms must have at least one word with 4+ characters (excluding common identifiers like BLOCK, UNIT, PHASE):

```python
has_specific = any(
    len(w) >= 4 and w not in {'BLOCK', 'UNIT', 'PHASE'}
    for w in words
)
```

---

## Document Grouping

### The Problem

ORI returns one row per party. Raw results look like:

```
[
  {"instrument": "2024123456", "person_type": "PARTY 1", "name": "SMITH JOHN", "doc_type": "(D) DEED"},
  {"instrument": "2024123456", "person_type": "PARTY 2", "name": "JONES MARY", "doc_type": "(D) DEED"},
  {"instrument": "2024123456", "person_type": "PARTY 2", "name": "JONES ROBERT", "doc_type": "(D) DEED"},
]
```

This is **3 records for 1 document**. If not grouped, the chain builder would see 3 separate deeds.

### The Solution

Group by instrument number and collect party names:

```python
def _group_ori_records_by_instrument(self, docs: list) -> list:
    by_instrument = {}
    for doc in docs:
        instrument = doc.get("instrument")
        if instrument not in by_instrument:
            by_instrument[instrument] = {
                "instrument": instrument,
                "doc_type": doc.get("doc_type"),
                "record_date": doc.get("record_date"),
                "party1_names": [],  # Grantor/Mortgagor/Debtor
                "party2_names": [],  # Grantee/Mortgagee/Creditor
            }

        person_type = doc.get("person_type", "").upper()
        name = doc.get("name", "").strip()

        if "PARTY 1" in person_type:
            by_instrument[instrument]["party1_names"].append(name)
        elif "PARTY 2" in person_type:
            by_instrument[instrument]["party2_names"].append(name)

    return list(by_instrument.values())
```

---

## Data Quality Issues

### CRITICAL: Search Results May Include Different Properties

**This is the most important data quality concern.**

When searching by legal description, ORI returns ALL documents that match the search pattern. This can include:

1. **Documents for the target property** (correct)
2. **Documents for adjacent lots in the same subdivision** (incorrect)
3. **Documents for similarly-named subdivisions** (incorrect)

**Example**: Searching for `TUSCANY*` might return:
- LOT 5 BLOCK 3 TUSCANY SUBDIVISION (target property)
- LOT 6 BLOCK 3 TUSCANY SUBDIVISION (neighbor)
- LOT 1 TUSCANY ESTATES (different subdivision entirely)

### Validation Required

Before building a chain of title, documents should be validated:

1. **Lot number match**: Does the document's legal description include the correct lot?
2. **Block number match**: Does the block match (if applicable)?
3. **Subdivision name match**: Is this the same subdivision?

**Current implementation does NOT do this validation.** Documents are grouped and saved regardless of whether they match the target property's exact legal description.

### Recommended Validation Approach

```python
def validate_document_match(doc_legal: str, target_legal: LegalDescription) -> bool:
    """Check if document's legal description matches target property."""
    doc_parsed = parse_legal_description(doc_legal)

    # Must match lot if target has lot
    if target_legal.lot and doc_parsed.lot != target_legal.lot:
        return False

    # Must match block if target has block
    if target_legal.block and doc_parsed.block != target_legal.block:
        return False

    # Subdivision should be similar (fuzzy match)
    if target_legal.subdivision and doc_parsed.subdivision:
        similarity = calculate_similarity(target_legal.subdivision, doc_parsed.subdivision)
        if similarity < 0.8:
            return False

    return True
```

### Missing Party 2 Data (Cross-Party Issue)

**This is a significant data quality issue that affects chain building.**

Some ORI records lack Party 2 (grantee) information. When we search by legal description, ORI returns only the indexed parties for that legal. If the grantee (Party 2) was not indexed against the same legal description, we won't find them.

**Example - Instrument 2024478600:**
```
Search: RETREAT ON DAVIS ISLAND*
Result: 1 record
  PARTY 1 | BARGAMIN KRISTEN H | (D) DEED | Inst: 2024478600
  # No Party 2 record returned
```

This deed has a grantor but no grantee in our results, which breaks chain building because we can't determine who acquired the property.

**Why This Happens:**

1. **Incomplete Indexing**: The clerk's office may not have finished indexing all parties for recent documents. The example above was recorded 11/19/2024 - possibly still being processed.

2. **Different Legal Descriptions**: Party 1 and Party 2 might be indexed under slightly different legal descriptions:
   - Party 1 indexed under: "RETREAT ON DAVIS ISLAND UNIT 202"
   - Party 2 indexed under: "UNIT 202 BLDG A RETREAT ON DAVIS ISLAND CONDO"

   Our search would find one but not the other.

3. **Historical Records**: Very old deeds (pre-1970s) often have incomplete party indexing.

4. **Special Deed Types**: Some deeds (like quitclaims removing a party, or corrective deeds) may legitimately only have one party indexed.

**Current Impact:**
- Deeds with missing Party 2 are saved to the database but cannot be used in chain building
- The chain has gaps where ownership transfers cannot be determined
- Success rate is reduced for properties with incomplete indexing

**Potential Solutions:**

1. **OCR the Document**: Download the actual deed PDF and use OCR to extract the grantee name

2. **Multiple Search Strategies**: Search using variations of the legal description to find Party 2 under different indexing

3. **Cross-Reference with HCPA**: The Property Appraiser often has the current owner, which can help identify the most recent grantee

4. **Fuzzy Party Matching**: If we find a deed where Party 1 matches a known previous owner, we might infer the chain even without Party 2

**Example of Split Indexing:**
```python
# Party 1 search result
{"legal": "RETREAT ON DAVIS ISLAND UNIT 202", "person_type": "PARTY 1", "name": "SMITH JOHN"}

# Party 2 might be indexed under a different legal
{"legal": "UNIT 202 RETREAT ON DAVIS ISLAND CONDOMINIUM", "person_type": "PARTY 2", "name": "JONES MARY"}
```

To find both, we'd need to search for:
- `RETREAT ON DAVIS*`
- `UNIT 202 RETREAT*`
- `RETREAT*CONDOMINIUM*`

And then group by instrument number across all results.

### Common Data Issues

| Issue | Cause | Impact |
|-------|-------|--------|
| Missing Party 2 | Older records not fully indexed | Chain has gaps |
| Wrong property in results | Search too broad | Incorrect documents in chain |
| Duplicate instruments | ORI returns party rows | Fixed by grouping |
| Inconsistent doc types | Clerk coding variations | May miss some liens |

---

## Known Patterns and Edge Cases

### Subdivision Name Variations

The same subdivision may appear with different names in recorded documents:

```
RETREAT AT CARROLLWOOD
THE RETREAT AT CARROLLWOOD
RETREAT AT CARROLLWOOD SUBDIVISION
RETREAT AT CARROLLWOOD SUB
RETREAT AT CARROLLWOOD A SUBDIVISION
```

Search using the shortest unique identifier: `RETREAT*` or `RETREAT AT CARROLLWOOD*`

### Condominium Legal Descriptions

Condos use UNIT instead of LOT:

```
UNIT 304 BUILDING A HARBOUR ISLAND CONDOMINIUM
U 304 BLDG A HARBOUR ISLAND CONDO
```

The parsing handles this with unit-specific patterns.

### Plat Book/Page References

Some legal descriptions include plat references:

```
LOT 5 BLOCK 3 TUSCANY SUBDIVISION PLAT BOOK 70 PAGE 7
```

These references are parsed but not currently used for searching (they reference the plat map, not individual documents).

### Section-Township-Range Edge Cases

```
23-24-25                    # Basic STR
NE 1/4 OF SEC 1-28-19       # Quarter section
S 1/2 OF NW 1/4 SEC 15      # Half of quarter
```

STR notation is less common in urban Hillsborough County but appears for rural parcels.

---

## API vs Browser Scraping

There are two methods to search ORI:

### 1. Browser Scraping (Current Primary Method)

**URL**: `https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=321&OBKey__1011_1={search_term}`

**Characteristics:**
- Returns one row per party per document
- Requires grouping by instrument number
- No result limit (returns all matches)
- Slower (requires Playwright browser automation)
- More prone to hanging/timeouts

**Example Response (74 rows for 8 documents):**
```
Instrument 2014318870 | PARTY 1 | STANDARD PACIFIC OF FLORIDA | (D) DEED
Instrument 2014318870 | PARTY 2 | MAIDA AMANDA | (D) DEED
Instrument 2014318870 | PARTY 2 | MAIDA CHRISTOPHER S | (D) DEED
... (71 more rows)
```

### 2. API Method (Preferred)

**URL**: `https://publicaccess.hillsclerk.com/Public/ORIUtilities/DocumentSearch/api/Search`

**Characteristics:**
- Returns one row per document with parties as arrays
- No grouping needed
- Has a 25-record limit (Truncated flag indicates if more exist)
- Faster (direct HTTP POST)
- More reliable

**Example Response (8 rows for 8 documents):**
```json
{
  "Success": true,
  "Truncated": null,
  "ResultList": [
    {
      "Instrument": "2014318870",
      "DocType": "(D) DEED",
      "Legal": "L 37 RETREAT AT CARROLLWOOD",
      "PartiesOne": ["STANDARD PACIFIC OF FLORIDA"],
      "PartiesTwo": ["MAIDA AMANDA", "MAIDA CHRISTOPHER S"]
    }
  ]
}
```

### Confirmed: Missing Party 2 is in ORI Database

Using the API, we confirmed that instrument 2024478600 genuinely has no grantee in ORI's database:

```json
{
  "Instrument": "2024478600",
  "DocType": "(D) DEED",
  "Legal": "RETREAT ON DAVIS ISLAND UNIT 202",
  "PartiesOne": ["BARGAMIN KRISTEN H"],
  "PartiesTwo": []  // Empty - no grantee indexed
}
```

This is a Clerk's office data entry issue, not a problem with our code.

### Recommendation

Consider switching to the API method as the primary search:
1. Cleaner data (no grouping needed)
2. Faster execution
3. More reliable
4. Parties already in arrays

The 25-record limit can be worked around by using more specific search terms.

---

## Implementation Files

- **Parsing Logic**: `src/utils/legal_description.py`
  - `parse_legal_description()` - Extracts components from raw text
  - `generate_search_permutations()` - Creates ORI search terms
  - `build_ori_search_terms()` - Main entry point for search term generation

- **Document Grouping**: `src/services/ingestion_service.py`
  - `_group_ori_records_by_instrument()` - Combines party rows into single documents
  - `_map_grouped_ori_doc()` - Maps grouped ORI data to database schema

- **Chain Building**: `src/services/title_chain_service.py`
  - `build_chain_and_analyze()` - Builds ownership chain from documents
  - `_build_deed_chain()` - Links deeds into ownership timeline

---

## Future Improvements

1. **Document Validation**: Implement validation to ensure search results match the target property's exact lot/block/subdivision before saving.

2. **Fuzzy Subdivision Matching**: Use string similarity to match subdivision name variations.

3. **Legal Description Normalization**: Expand abbreviations to canonical forms for better matching.

4. **OCR Integration**: Extract legal descriptions from scanned PDFs when metadata is incomplete.

5. **Duplicate Detection**: Identify when the same document is returned in multiple searches.

---

## Testing Legal Description Parsing

```python
from src.utils.legal_description import parse_legal_description, generate_search_permutations

# Test a legal description
legal = "LOT 27 BLOCK D WILLOW BEND SUBDIVISION"
parsed = parse_legal_description(legal)
print(f"Lot: {parsed.lot}")           # 27
print(f"Block: {parsed.block}")       # D
print(f"Subdivision: {parsed.subdivision}")  # WILLOW BEND SUBDIVISION

# Generate search terms
terms = generate_search_permutations(parsed)
print(f"Search terms: {terms}")
# ['WILLOW*', 'L 27 WILLOW*', 'LOT 27 WILLOW*', 'L 27 B D WILLOW*', ...]
```

---

## Appendix: Regex Patterns Used

### Lot Extraction
```python
lot_patterns = [
    r'\bLOT\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',  # LOT 5, LOT J, LOT 5A
    r'\bL\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',     # L 5
    r'\bLT\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',    # LT 5
]
```

### Block Extraction
```python
block_patterns = [
    r'\bBLOCK\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',  # BLOCK D, BLOCK 3
    r'\bBLK\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',
    r'\bB\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',
]
```

### Subdivision Extraction (in priority order)
```python
subdiv_patterns = [
    # Match PHASE-suffixed names first (before PHASE is removed)
    r'\b([A-Z]{2}[A-Z\s]+PHASE\s+\d+[A-Z]?)\b',
    # Match SECTION-numbered subdivisions
    r'\b([A-Z]{2}[A-Z\s]*\s+SECTION\s+\d+[A-Z]?)\b',
    # Match standard subdivision suffixes
    r'\b([A-Z]{2}[A-Z\s]*(?:SUBDIVISION|SUBDIV|SUBD|SUB|S/D|...))\b',
]
```

### Removal Patterns (cleaned before subdivision search)
```python
removal_patterns = [
    r'\bLOT\s+[A-Z]?\d+[A-Z]?\b',
    r'\bBLOCK\s+[A-Z]?\d*[A-Z]?\b',  # Handles BLOCK D
    r'\bUNIT\s+(?:NO\s+)?\d+[A-Z]?\b',
    r'\bPHASE\s+\d+[A-Z]?\b',
    r'\d+-\d+-\d+',  # Section-Township-Range
    r'PLAT\s+BOOK\s+\d+\s+PAGE\s+\d+',
]
```
