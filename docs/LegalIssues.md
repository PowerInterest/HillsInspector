# Legal Description Parsing Analysis

**Date:** 2025-12-23
**Last Updated:** 2025-12-23
**Purpose:** Document legal description patterns that parse well vs poorly to guide parser improvements.

## Summary

| Category | Count | Description |
|----------|-------|-------------|
| Poor Parsing (<3 docs) | 46 | Legal descriptions that resulted in few/no ORI documents |
| Medium (3-9 docs) | 29 | Adequate but not optimal results |
| Good Parsing (10+ docs) | 13 | Legal descriptions that worked well |

---

## ✅ Implemented Fixes (2025-12-23)

The following parser improvements have been implemented in `src/utils/legal_description.py`:

### 1. Multiple Lots Extraction (Priority 1) ✅
**File:** `src/utils/legal_description.py` lines 117-139

**Before:** `LOTS 1, 2 AND 3` → `lots=['1']` (only first lot)
**After:** `LOTS 1, 2 AND 3` → `lots=['1', '2', '3']` (all lots)

```python
# New pattern captures ALL lot numbers after LOTS keyword
lots_multi_match = re.search(
    r'\bLOTS\s+((?:[A-Z]?\d+[A-Z]?\s*(?:,|AND|\s)\s*)+[A-Z]?\d+[A-Z]?)', text
)
if lots_multi_match:
    lot_nums = re.findall(r'\b([A-Z]?\d+[A-Z]?)\b', lots_multi_match.group(1))
    for num in lot_nums:
        if num and num not in lots_found:
            lots_found.append(num)
```

### 2. Subdivision After Block Pattern (Priority 4) ✅
**File:** `src/utils/legal_description.py` lines 261-276

**Before:** `LOT 15, BLOCK 17, NORTHDALE, SECTION B` → `subdivision=None`
**After:** → `subdivision='NORTHDALE'`

Added fallback to extract subdivision name that appears AFTER "BLOCK X," pattern.

### 3. Subdivision After Lot Pattern (New) ✅
**File:** `src/utils/legal_description.py` lines 278-290

**Before:** `LOT 11, WOODARD'S MANOR, ACCORDING...` → `subdivision=None`
**After:** → `subdivision="WOODARD'S MANOR"`

Added fallback for "LOT X, SUBDIVISION, ACCORDING..." pattern (no block).

### 4. Apostrophe Normalization (Priority 4) ✅
**File:** `src/utils/legal_description.py` lines 333-359

**Before:** `TURMAN'S` → only generates search term "TURMAN S"
**After:** → generates both "TURMAN S" AND "TURMANS"

ORI may index names with or without apostrophes, so we now try both forms.

### Test Results After Implementation
- **Multiple Lots:** 6/6 tests passed (was 4/6)
- **Subdivision Extraction:** 9/9 tests passed (was 8/9)
- **Regression Tests:** All 13 good cases continue to work correctly

### Test Suite Location
Test cases saved to: `docs/legal_test_cases.json`
Test harness: `tests/test_legal_parser.py`

---

## Poor Parsing Examples (<3 documents)

These legal descriptions resulted in 0-2 documents being found. The patterns here need parser improvements.

### Category 1: Metes-and-Bounds Descriptions (0 docs)

These have no subdivision/lot/block structure - pure boundary descriptions.

| Folio | Legal Description | Docs | Issue |
|-------|-------------------|------|-------|
| `203011ZZZ000002872400U` | BEG 1359.68 FT S AND 30 FT E OF NW COR OF NW 1/4 AND RUN E 186.23 FT S 281 FT W 186.23 FT AND N 281 FT TO BEG | 0 | No subdivision pattern - metes-and-bounds only |
| `172716ZZZ000000103700U` | COM AT NW COR OF NW 1/4 RUN S 608.2 FT TO A PT ON SLY R/W LINE OF TARPON SPRINGS RD... | 0 | Pure metes-and-bounds, no lot/block |
| `172814ZZZ000000315400U` | TRACT DESC AS FROM NW COR OF SW 1/4 OF NW 1/4 RUN S 01 DEG 10 MIN 33 SEC W ALG W BDRY 250 FT... | 0 | "TRACT DESC AS" followed by bearings |
| `192935663000001641100U` | SOUTH TAMPA SUBDIVISION W 72.15 FT OF E 567.9 FT OF N 140 FT OF TRACT 9 IN NW 1/4 LESS N 10 FT FOR RD | 0 | Complex partial tract with dimensions |
| `202817ZZZ000002083000U` | E 126 FT OF W 963 FT OF N 1/4 OF NW 1/4 OF SW 1/4 LESS N 25 FT FOR RD | 0 | Quarter-section description with offsets |

**Pattern:** ZZZ in folio often indicates metes-and-bounds (unplatted land)

### Category 2: Multiple Lots Pattern (0 docs)

Parser extracts only first lot, but property spans multiple lots.

| Folio | Legal Description | Docs | Issue |
|-------|-------------------|------|-------|
| `18301742J000100000010A` | PORT TAMPA CITY MAP **LOTS 1 2 AND 3** LESS W 50 FT THEREOF BLOCK 100 | 0 | Parser extracts lot=1 only, misses lots 2,3 |
| `192818453R00000000010A` | CASTLE HEIGHTS MAP **LOTS 1 AND 2** BLOCK R | 0 | "LOTS X AND Y" pattern not parsed |
| `1729020FM000025000180U` | TOWN'N COUNTRY PARK UNIT NO 06 **E 13 FT OF LOT 18 AND W 52 FT OF LOT 19** BLOCK 25 | 1 | Partial lots spanning two lot numbers |
| `1929074UM000001000120A` | PANAMA **LOT 12 AND S 1/2 OF CLOSED ALLEY** BLOCK 1 | 18 | (Actually works well - "S 1/2 OF CLOSED ALLEY" ignored) |

**Pattern:** "LOTS X Y AND Z" or "LOT X AND ... LOT Y" needs to extract ALL lot numbers.

### Category 3: Partial Lot Descriptions (0-1 docs)

Lot number exists but with dimensional qualifiers that may not appear in ORI.

| Folio | Legal Description | Docs | Issue |
|-------|-------------------|------|-------|
| `1932071V5000000001250U` | RUSKIN CITY MAP OF **N 120 FT OF LOT 125** | 0 | "N 120 FT OF" prefix before lot |
| `19290549Y000005000131A` | BELLMONT HEIGHTS **N 54 FT OF LOT 13** BLOCK 5 | 1 | "N 54 FT OF" qualifier |
| `1829144PP000008000110A` | MUNRO'S AND CLEWIS'S ADDITION TO WEST TAMPA LOT 11 BLOCK 8 | 1 | Apostrophes in subdivision name |

**Pattern:** "N/S/E/W X FT OF LOT Y" - the direction+distance qualifier may not be in ORI records.

### Category 4: Complex Phase/Unit Naming (0 docs)

Subdivision names with complex phase notations.

| Folio | Legal Description | Docs | Issue |
|-------|-------------------|------|-------|
| `2032089YQ000024000060U` | SUNSHINE VILLAGE **PHASES 1A-1/1B-1/1C** LOT 6 BLOCK 24 | 0 | Complex phase notation "1A-1/1B-1/1C" |
| `1827320R2000017000150U` | NORTHDALE **SECTION B UNIT NO 2** LOT 15 BLOCK 17 | 0 | "SECTION B UNIT NO 2" in name |

**Pattern:** Phase notations with slashes, hyphens, or "SECTION X UNIT Y" may not match ORI format.

### Category 5: Simple Patterns That Still Failed (0 docs)

These look like they should work but didn't - needs investigation.

| Folio | Legal Description | Docs | Issue |
|-------|-------------------|------|-------|
| `1830093YD000010000110A` | GANDY MANOR ADDITION LOT 11 BLOCK 10 | 0 | Simple pattern - why no docs? |
| `1928071GR000000000090U` | FLOWERS AND STUART OAK GROVE SUBDIVISION LOT 9 | 0 | Simple pattern - check ORI data |
| `1928131IO000001000400U` | TEMPLE OAKS LOT 40 BLOCK 1 | 0 | Simple pattern - verify ORI |
| `1929174WA000030000130A` | TURMAN'S EAST YBOR LOT 13 BLOCK 30 | 0 | Apostrophe in name |

**Action:** These need manual ORI verification to determine if issue is parser or data availability.

---

## Good Parsing Examples (10+ documents)

These legal descriptions successfully retrieved many documents. Study these patterns.

### Simple Subdivision + Lot + Block

| Folio | Legal Description | Docs | Pattern |
|-------|-------------------|------|---------|
| `21300836P000002000020U` | RIVER RIDGE RESERVE LOT 2 BLOCK 2 | 38 | SUBDIVISION LOT X BLOCK Y |
| `21282730G000000000110U` | WOODARD'S MANOR LOT 11 | 24 | SUBDIVISION LOT X (no block) |
| `2030192RX000000000260U` | KENLAKE SUBDIVISION LOT 26 | 22 | SUBDIVISION LOT X |
| `193120773000000000200U` | SUNSET BAY TOWNHOMES LOT 20 | 29 | SUBDIVISION LOT X |
| `2029112A2000001000480U` | OAK FOREST ADDITION LOT 48 BLOCK 1 | 10 | SUBDIVISION LOT X BLOCK Y |
| `2029132AL000002000090U` | BRANDON LAKES LOT 9 BLOCK 2 | 13 | SUBDIVISION LOT X BLOCK Y |

### With Phase/Unit (Working)

| Folio | Legal Description | Docs | Pattern |
|-------|-------------------|------|---------|
| `20271089U000029000050A` | HERITAGE ISLES PHASE 2A LOT 5 BLOCK 29 | 16 | PHASE XY format works |
| `2031169Y5000000000460U` | SOUTH FORK TRACT N LOT 46 | 17 | TRACT X format works |
| `1928245UC000001000020T` | BRIDGEFORD OAKS PHASE 2 LOT 2 BLOCK 1 | 19 | Simple PHASE X works |
| `213007369000000000230U` | LITTLE OAK ESTATES UNIT 2 LOT 23 | 14 | UNIT X format works |
| `203109736000005000160U` | SUMMERFIELD VILL I TRACT 21 UNIT 2 PHS 3A/3B LOT 16 BLOCK 5 | 16 | Complex but worked |

### Metes-and-Bounds That Worked

| Folio | Legal Description | Docs | Notes |
|-------|-------------------|------|-------|
| `193024ZZZ000001717900U` | N 80 FT OF S 553 FT OF W 170 FT OF E 800 FT OF GOV LOT 8 | 134 | "GOV LOT 8" gave anchor point |

---

## Key Observations

### What Works Well
1. **Simple format:** `SUBDIVISION LOT X BLOCK Y`
2. **Single phase/unit:** `SUBDIVISION PHASE X LOT Y BLOCK Z`
3. **No partial measurements:** Full lots, not "N 50 FT OF LOT X"
4. **Clean names:** No special characters or complex phase notation

### What Fails
1. **Multiple lots:** "LOTS 1 2 AND 3" - parser only gets first lot
2. **Partial lots:** "N 54 FT OF LOT 13" - ORI may not have this detail
3. **Complex phases:** "PHASES 1A-1/1B-1/1C" - too specific
4. **Pure metes-and-bounds:** No subdivision anchor at all
5. **Apostrophes:** "MUNRO'S" may not match ORI format

### Folio Pattern Insight
- Folios ending in `ZZZ` often indicate unplatted/metes-and-bounds land
- These properties may have limited ORI indexing by legal description
- May need party-based search fallback for ZZZ folios

---

## Recommended Parser Improvements

### Priority 1: Multiple Lots
```
Current:  "LOTS 1 2 AND 3" -> lot=1
Needed:   "LOTS 1 2 AND 3" -> lots=[1,2,3]
```

**Current Code Issue** (`src/utils/legal_description.py:111`):
```python
r'\bLOTS\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',  # LOTS 5, LOTS J (captures first)
```
This regex only captures the FIRST number after "LOTS".

**Fix Required:**
```python
# New pattern to extract all lot numbers after LOTS
lots_multi_match = re.search(r'\bLOTS\s+([\d\s,AND]+)', text, re.IGNORECASE)
if lots_multi_match:
    # Extract all numbers from "1 2 AND 3" or "1, 2, 3"
    lot_nums = re.findall(r'\d+', lots_multi_match.group(1))
    for num in lot_nums:
        if num not in lots_found:
            lots_found.append(num)
```

**Search Strategy Change** (`generate_search_permutations`):
- Currently iterates `lots_to_use` and generates `L {lot} B {block}` for each
- Good - but ORI filter may reject if doc only mentions one lot
- May need to relax filter: accept doc if ANY lot matches, not all

---

### Priority 2: Partial Lot Handling
```
Current:  "N 54 FT OF LOT 13" -> lot=13 (correct extraction)
Issue:    ORI docs indexed as "LOT 13" without "N 54 FT OF"
Result:   Filter rejects because legal doesn't match exactly
```

**Root Cause**: Not a parser issue - it's a filter stringency issue in ORI search.

**Fix Required** in `src/scrapers/ori_api_scraper.py` or filter logic:
- When filtering results, compare lot/block numbers only
- Ignore directional prefixes (N/S/E/W X FT OF)
- Consider partial lots as matches if lot number is present

---

### Priority 3: Metes-and-Bounds Fallback

**Detection**: Folio pattern `ZZZ` in position 7-9 indicates unplatted land:
```python
def is_metes_and_bounds(folio: str, legal: str) -> bool:
    # ZZZ in folio = unplatted
    if len(folio) >= 9 and folio[6:9] == 'ZZZ':
        return True
    # Or no LOT/BLOCK keywords
    if 'LOT' not in legal.upper() and 'L ' not in legal.upper():
        return True
    return False
```

**Fallback Strategy**:
1. If metes-and-bounds detected, skip legal description search
2. Use owner name + address search instead
3. Or search by broader area descriptors (Section-Township-Range)

---

### Priority 4: Subdivision Name Normalization

**Apostrophe Handling** (`_subdivision_prefixes` at line 291):
```python
# Current: re.findall(r"[A-Z0-9]+", ...) strips apostrophes
# "MUNRO'S" -> ["MUNRO", "S"]

# Fix: Also generate version without trailing letter
prefixes.append("MUNROS")  # Apostrophe stripped entirely
prefixes.append("MUNRO S")  # Current behavior
```

**Complex Phase Simplification**:
```python
# "PHASES 1A-1/1B-1/1C" -> try multiple approaches:
1. Extract base: "SUNSHINE VILLAGE" (ignore phase entirely)
2. Extract first phase: "SUNSHINE VILLAGE PHASE 1"
3. Try each phase variant: "PHASE 1A", "PHASE 1B", "PHASE 1C"
```

---

## Code Analysis: Current Parser Flow

### `parse_legal_description()` (line 87-265)
1. Strips leading section numbers (good)
2. Extracts lots via regex patterns (limited to first lot for LOTS pattern)
3. Extracts block, unit, phase (works for simple cases)
4. Extracts subdivision name via suffix patterns (works well)

### `generate_search_permutations()` (line 268-449)
1. Generates `L {lot} B {block} {subdiv}*` format (ORI-optimized)
2. Iterates through `lots_to_use` (good - uses all detected lots)
3. Falls back to metes-and-bounds handling with 60-char prefix
4. Road name extraction for metes-and-bounds (smart)

### `build_ori_search_terms()` (line 501-629)
1. Prioritizes Final Judgment legal description
2. Falls back to bulk data legal
3. Filters generic terms (good)
4. Filters year-only and measurement terms (good)

---

## Implementation Plan

### Phase 1: Multiple Lots (Highest Impact)
**File:** `src/utils/legal_description.py`
**Lines:** 107-134

Add new pattern after existing lot patterns:
```python
# Pattern for "LOTS X Y AND Z" or "LOTS X, Y, Z"
lots_multi = re.search(r'\bLOTS\s+((?:\d+\s*(?:,|AND|\s)*)+)', text)
if lots_multi:
    nums = re.findall(r'\d+', lots_multi.group(1))
    for n in nums:
        if n not in lots_found:
            lots_found.append(n)
```

### Phase 2: Partial Lot Relaxation
**File:** `src/scrapers/ori_api_scraper.py` (filter logic)

When comparing legal descriptions:
```python
def is_partial_lot_match(parsed_legal, doc_legal):
    # Strip directional prefixes before comparison
    doc_lot = extract_lot_number(doc_legal)
    return doc_lot in parsed_legal.lots
```

### Phase 3: ZZZ Folio Detection
**File:** `src/orchestrator.py` (metes-and-bounds fallback)

Add early detection before legal parsing:
```python
if folio[6:9] == 'ZZZ' or 'LOT' not in legal.upper():
    # Use owner name search instead
    terms = [(owner_name, None)]  # party search tuple
```

### Phase 4: Apostrophe Normalization
**File:** `src/utils/legal_description.py:291`

Add alternative prefixes:
```python
# After generating prefixes, add apostrophe-stripped versions
stripped = re.sub(r"'S?\b", "", subdivision)  # Remove 's or '
if stripped != subdivision:
    prefixes.append(stripped.upper())
```

---

## Test Cases

After implementation, verify against these examples:

| Folio | Legal | Expected Behavior |
|-------|-------|-------------------|
| `18301742J000100000010A` | LOTS 1 2 AND 3 BLOCK 100 | lots=[1,2,3], search all |
| `19290549Y000005000131A` | N 54 FT OF LOT 13 BLOCK 5 | lot=13, partial match OK |
| `203011ZZZ000002872400U` | BEG 1359.68 FT S... | Detect ZZZ, use owner search |
| `1829144PP000008000110A` | MUNRO'S AND CLEWIS'S | Try MUNROS, MUNRO S |
| `2032089YQ000024000060U` | PHASES 1A-1/1B-1/1C | Extract base subdiv, broader search |

---

## Next Steps

### Completed ✅
1. [x] Implement multiple lot extraction (Phase 1) - **DONE 2025-12-23**
2. [x] Implement apostrophe normalization (Phase 4) - **DONE 2025-12-23**
3. [x] Add subdivision extraction for "LOT X, BLOCK Y, NAME" pattern - **DONE 2025-12-23**
4. [x] Add subdivision extraction for "LOT X, NAME" pattern (no block) - **DONE 2025-12-23**
5. [x] Test parser changes against this document's examples - **DONE 2025-12-23**

### Remaining
1. [ ] Manually verify ORI data exists for "simple pattern failures" (31 cases parse correctly but return 0 docs)
2. [ ] Add relaxed filtering for partial lots (Phase 2) - ORI filter issue, not parser
3. [ ] Add ZZZ folio detection for metes-and-bounds fallback (Phase 3) - Orchestrator change needed

### Notes on Remaining Issues
- **31 "simple_fail" cases** parse correctly with proper lot/block/subdivision extraction, but still return 0 documents. This suggests the issue is ORI data availability, not the parser.
- **6 metes-and-bounds cases** (ZZZ folios) have no lot/block structure. These need party-name search fallback in the orchestrator, not parser changes.
- **Partial lots** (e.g., "N 54 FT OF LOT 13") extract correctly but may need ORI filter relaxation to match docs indexed without the directional prefix.

---

## ✅ ORI Search Format Discovery (2025-12-23)

After extensive testing, we discovered the exact format ORI uses for indexing legal descriptions.

### Key Finding: Multi-Lot Format

**ORI uses `L 1 AND 2 B R` format** - not separate entries for each lot.

Example - CASTLE HEIGHTS (folio `192818453R00000000010A`):
- **HCPA Legal:** `LOTS 1 AND 2, BLOCK R, MAP OF CASTLE HEIGHTS`
- **ORI Indexed:** `L 1 AND 2 B R MAP OF CASTLE HEIGHTS`

Our original search `L 1 B R CASTLE HEIGHTS*` returned 0 results.
The correct search `L 1 AND 2 B R*` returned 4 documents!

### Search Term Spacing Matters

The API uses `CONTAINS` search, which requires exact substring matching:

| Search Term | API Results |
|------------|-------------|
| `L 40 B 1 TEMPLE OAKS` | **14 results** ✓ (spaces after L/B) |
| `L40 B1 TEMPLE OAKS` | 0 results ✗ (no spaces) |
| `L 1 AND 2 B R` | **4 results** ✓ (multi-lot with AND) |
| `L 1 B R CASTLE` | 0 results ✗ (missing "AND 2") |

### Implemented Fixes

**1. Multi-lot search format** (`src/utils/legal_description.py` lines 389-400):
```python
# Priority 0: Multi-lot combined format (ORI uses "L 1 AND 2 B R" format)
if len(lots_to_use) >= 2 and legal.block:
    combined_lots = " AND ".join(lots_to_use[:2])
    permutations.append(f"L {combined_lots} B {legal.block}*")
    permutations.append(f"L {combined_lots} B {legal.block} MAP*")
```

**2. Filter fix for "AND X" patterns** (`src/services/ingestion_service.py` lines 1257-1262):
```python
# Primary pattern: "L 1" or "LOT 1"
lot_pattern = rf'\bL(?:OT)?\s*{re.escape(lot)}\b'
# Secondary pattern: "AND 2" or ", 2" for multi-lot
lot_and_pattern = rf'\b(?:AND|,)\s*{re.escape(lot)}\b'
if re.search(lot_pattern, legal_upper) or re.search(lot_and_pattern, legal_upper):
    lot_hits += 1
```

**3. Search term order**: With-spaces format tried first for API compatibility.

### Result

CASTLE HEIGHTS went from **0 documents** to **4 documents**:
- (MTG) MORTGAGE 2023192865 - 2023-05-05
- (NOC) NOTICE OF COMMENCEMENT 2022606019 - 2022-12-28
- (D) DEED 2022053069 - 2022-01-31
- (MTG) MORTGAGE 2022053070 - 2022-01-31

---

## Partial Lot Discovery (2025-12-23)

ORI uses different formats for partial lots (those with "LESS THE", "N 50 FT OF", etc.):

### Key Finding: Range Notation for Partial Lots

**PORT TAMPA CITY** (folio `18301742J000100000010A`):
- **HCPA Legal:** `LOTS 1, 2 AND 3, LESS THE WEST 50 FEET THEREOF, IN BLOCK 100, MAP OF PART OF PORT TAMPA CITY`
- **ORI Indexed:** `PT L 1-3 B 100 PORT TAMPA CITY MAP`

The "PT" prefix indicates "partial" and the "1-3" is range notation for consecutive lots.

| Legal Description | ORI Format |
|------------------|------------|
| Full lots: "LOTS 1 AND 2" | `L 1 AND 2 B R` |
| Partial lots: "LOTS 1, 2 AND 3 LESS THE WEST 50 FT" | `PT L 1-3 B 100` |

### Implemented Fixes

**1. Partial lot detection** (`src/utils/legal_description.py` lines 389-396):
```python
# Detect partial lots (keywords indicating less than full lot)
is_partial_lot = any(kw in raw_legal_upper for kw in [
    "LESS THE", "LESS ", " PART OF", "PORTION OF", " PT OF",
    " N ", " S ", " E ", " W ",  # Directional partials
    "NORTH ", "SOUTH ", "EAST ", "WEST ",
])
partial_prefix = "PT " if is_partial_lot else ""
```

**2. Range notation for consecutive lots** (`src/utils/legal_description.py` lines 398-418):
```python
# For partial/consecutive lots, use range notation (e.g., "PT L 1-3 B 100")
if is_partial_lot and are_consecutive(lots_to_use):
    lot_range = f"{lots_to_use[0]}-{lots_to_use[-1]}"
    permutations.append(f"PT L {lot_range} B {legal.block}*")
    permutations.append(f"L {lot_range} B {legal.block}*")  # Also try without PT
```

**3. Filter support for range notation** (`src/services/ingestion_service.py` lines 1261-1267):
```python
# Range pattern: "L 1-3" covers lots 1, 2, 3 (check if lot is in range)
range_pattern = re.search(r'\bL(?:OT)?\s*(\d+)-(\d+)\b', legal_upper)
if range_pattern and lot.isdigit():
    range_start, range_end = int(range_pattern.group(1)), int(range_pattern.group(2))
    if range_start <= int(lot) <= range_end:
        range_match = True
```

### Result

PORT TAMPA CITY went from **0 documents** to **2 documents**:
- (D) DEED 2000037314 - 2000-02-03
- (MTG) MORTGAGE 2020004537 - 2020-01-07

---

## Alternative Search Strategy: Name-Based Search

When legal description search fails, we can search by party name using the ORI API.

### ORI API Endpoint

**URL:** `https://publicaccess.hillsclerk.com/Public/ORIUtilities/DocumentSearch/api/Search`

**Method:** POST with JSON payload

**Headers:**
```json
{
  "Content-Type": "application/json; charset=UTF-8",
  "Accept": "application/json, text/javascript, */*; q=0.01",
  "Origin": "https://publicaccess.hillsclerk.com",
  "Referer": "https://publicaccess.hillsclerk.com/oripublicaccess/",
  "X-Requested-With": "XMLHttpRequest"
}
```

### Search by Legal Description

```json
{
  "DocType": ["(D) DEED", "(MTG) MORTGAGE", "(SAT) SATISFACTION", "(LN) LIEN"],
  "RecordDateBegin": "01/01/1900",
  "RecordDateEnd": "12/23/2025",
  "Legal": ["CONTAINS", "L 1 AND 2 B R"]
}
```

### Search by Party Name

```json
{
  "DocType": ["(D) DEED", "(MTG) MORTGAGE"],
  "RecordDateBegin": "01/01/2000",
  "RecordDateEnd": "12/23/2025",
  "Party": "SMITH JOHN"
}
```

**Note:** Party names are in `LAST FIRST MIDDLE` format.

### Response Format (JSON)

```json
{
  "ResultList": [
    {
      "ID": "...",
      "DocType": "(MTG) MORTGAGE",
      "Instrument": "2023192865",
      "RecordDate": 1683244800000,
      "Legal": "L 1 AND 2 B R MAP OF CASTLE HEIGHTS",
      "PartiesOne": ["CEDANO TIFFANY MARIAN"],
      "PartiesTwo": ["EQUITY PRIME MORTGAGE, LLC", "MERS"],
      "Book": "...",
      "Page": "..."
    }
  ]
}
```

### Best Practice: Name Search → Legal Extraction

When legal description parsing fails:

1. **Search by owner name** from HCPA parcel data
2. **Get JSON results** with document details
3. **Extract the ORI-indexed legal description** from a matching document
4. **Use that legal description** for subsequent searches

This bypasses parser limitations by using ORI's own indexed format.

**Example workflow:**
```python
# 1. Legal search failed - try owner name
results = ori_api.search_by_party("CEDANO TIFFANY")

# 2. Filter to relevant property (by address or subdivision keywords)
matching_doc = next(r for r in results if "CASTLE HEIGHTS" in r["Legal"])

# 3. Extract ORI's indexed legal format
ori_legal = matching_doc["Legal"]  # "L 1 AND 2 B R MAP OF CASTLE HEIGHTS"

# 4. Use ORI's format for comprehensive search
all_docs = ori_api.search_by_legal(ori_legal)
```

### When to Use Name Search

- Metes-and-bounds descriptions (ZZZ folios)
- Complex phase notations that don't match ORI indexing
- Properties where legal search returns 0 results
- As a verification fallback for critical properties

---
