# Legal Description Parsing Analysis

**Date:** 2025-12-23
**Last Updated:** 2025-12-26
**Purpose:** Document legal description patterns that parse well vs poorly to guide parser improvements.

## Summary

| Category | Count | Description |
|----------|-------|-------------|
| Poor Parsing (<3 docs) | 46 | Legal descriptions that resulted in few/no ORI documents |
| Medium (3-9 docs) | 29 | Adequate but not optimal results |
| Good Parsing (10+ docs) | 13 | Legal descriptions that worked well |

---

## 🎯 ORI Search Source Priority (Critical)

**Last Updated:** 2025-12-26

When searching ORI for documents, the order of search sources matters enormously. **Exact lookups (Book/Page, Instrument) should ALWAYS be exhausted before generating candidate search permutations.**

### Why Priority Matters

The problem with generated search terms (e.g., `L 4 B 8 TOUCHSTONE*`) is they can match wrong properties:
- `L 4 B 8` alone matches Lot 4 Block 8 in ANY subdivision
- Wildcard searches can match Lots 4, 40, 41, 42...
- Name searches cascade to find documents for unrelated properties

**Result:** Properties end up with 600-1500 chain periods instead of 10-30.

### Search Source Hierarchy

The following sources should be searched **in priority order**, exhausting each tier before moving to the next:

#### Tier 1: Exact Lookups (No False Positives)

| Source | Type | Why Reliable |
|--------|------|--------------|
| **HCPA Sales History Instrument #** | Instrument | Modern recordings (post ~2010) - exact lookup |
| **HCPA Sales History Book/Page** | Book/Page | Older recordings (pre ~2010) - exact lookup |
| **Final Judgment Plat Book/Page** | Book/Page | Exact plat lookup from recorded legal |
| **Foreclosed Mortgage Book/Page** | Book/Page | Exact lookup of mortgage being foreclosed |
| **Lis Pendens Instrument #** | Instrument | Exact lookup of foreclosure filing |

Book/Page and Instrument searches are **unambiguous** - they return exactly the document requested.

**Important:** Hillsborough County transitioned from Book/Page to Instrument-only recording around 2010. HCPA Sales History returns:
- **Modern recordings:** Instrument # only (`book` and `page` are null)
- **Older recordings:** Book/Page only (`instrument` may be null)

**We search for BOTH** to cover the full history of any property.

**Critical:** HCPA Sales History can contain **pre-plat sales of the parent parcel**. The HCPA folio tracks the current lot, but its sales history may include transactions for the raw land before subdivision. This is why the **plat search must come first** - it establishes the date boundary. Any HCPA instrument dated before the plat is for the parent parcel, not the current lot.

#### Tier 1.5: Case Number Search (Has False Positives!)

| Source | Type | Risk |
|--------|------|------|
| **Case Number** | Case # | ⚠️ **MEDIUM** - Can return adjacent case numbers |

**Warning (Discovered 2025-12-26):** ORI's case number index has a bug where documents from adjacent case numbers (filed same day) get cross-indexed. See "Critical Bug: ORI Case Number Index Cross-Contamination" section below.

**Mitigation:** Always validate case number results against legal description. Filter out documents where the legal doesn't match the property.

#### Tier 2: High-Confidence Legal Descriptions

| Source | Why Reliable |
|--------|--------------|
| **Final Judgment Legal Description** | Most accurate - an incorrect legal would halt the auction. Courts verify this. |
| **HCPA Legal Description** | County Appraiser's official record, tied to folio |
| **Bulk Parcel raw_legal fields** | County GIS data, generally accurate |

These should be searched using the **exact text** first, then parsed variations - but still with strict lot/block filtering.

#### Tier 3: Generated Search Permutations (Last Resort)

| Source | Risk Level |
|--------|------------|
| Generated variations (wildcards) | HIGH - Can match wrong lots/subdivisions |
| Party name searches | HIGH - Common names match many properties |

**These should only be used after Tier 1 and Tier 2 are exhausted**, and results must be strictly filtered.

### Implementation Status

| Feature | Status | Details |
|---------|--------|---------|
| **Plat Book/Page from FJ** | ✅ Implemented | `search_queue.py` queues plat book/page as Tier 1 search |
| **HCPA Sales History Instrument #** | ✅ Implemented | `search_queue.py` queues both instrument (modern) and book/page (older) |
| **Search priority order** | ✅ Implemented | Tier 1 (exact lookups) queued before Tier 2/3, priority values enforce execution order |

### What's NOT Available (By Design)

| Field | Status | Explanation |
|-------|--------|-------------|
| `foreclosed_mortgage.recording_book/page` | ❌ Not in FJ | Standard Florida "Uniform Final Judgment of Foreclosure" form doesn't include the original mortgage OR reference. Would need to extract from Lis Pendens or Complaint. |
| `lis_pendens.instrument_number` | ❌ Not in FJ | LP is a separate document filed at case start. The FJ references the LP but doesn't include its recording info. |

**Note:** HCPA Sales History has both book/page (older records) AND instrument numbers (modern records). We now search for BOTH.

### Plat Book/Page: The Root of Title

**Coverage:** 95.6% of Final Judgments (87/91) have `plat_book` and `plat_page` extracted.

The subdivision plat is critically important because:

1. **Root of Title** - The plat recording is when the lots legally come into existence. This is the absolute beginning of the chain of title for any platted property.

2. **Exact Lookup** - Book/Page search returns exactly one document with zero false positives.

3. **Chain Terminus** - Once we find the plat, we know we've reached the root. No need to search further back - there are no prior owners of "Lot 4 Block 8" before the plat was recorded.

4. **Date Anchor** - The plat recording date gives us the exact starting date for the chain of title. For MRTA analysis, this establishes the maximum possible chain length.

#### Critical Discovery: Plat Book Type (2025-12-26)

**Plats use a DIFFERENT book type in ORI than regular Official Records!**

| Book Type | OBKey Code | Description |
|-----------|------------|-------------|
| `OR` | Official Records | Standard deeds, mortgages, liens, etc. |
| `P` | Subdivision Plat Map | **Plat documents require this book type** |

When searching ORI by Book/Page, you MUST specify the correct book type:
- Regular documents: `book_type="OR"` (default)
- Plat documents: `book_type="P"`

**Example: TOUCHSTONE PHASE 2 (Plat Book 135, Page 12)**

ORI Search Result:
| Field | Value |
|-------|-------|
| Name | TOUCHSTONE PHASE 2 |
| Developer | LENNAR HOMES LLC |
| Recording Date | **2019-04-04** |
| Doc Type | (PL) PLAT |
| Plat Book/Page | 135/12 |
| Instrument # | 2019141481 |

This tells us:
- **Chain starts:** April 4, 2019 (when the lots legally came into existence)
- **Developer:** Lennar Homes LLC (first deed for any lot will be FROM Lennar)
- **Contamination filter:** Any document claiming "Lot 4 Block 8 Touchstone Phase 2" BEFORE this date is cross-property contamination

#### Plat PDF Access

**Direct Link Format:**
```
https://publicaccess.hillsclerk.com/oripublicaccess/?instrument={INSTRUMENT_NUMBER}
```

Example: `https://publicaccess.hillsclerk.com/oripublicaccess/?instrument=2019141481`

**Important Notes:**
- Plat PDFs are typically **8+ pages** (large survey maps)
- Loading can take **over 1 minute** for large plats
- If downloading plat PDFs, use **async downloads** to avoid blocking
- We don't need to download the plat PDF for chain analysis - just capture the **instrument number** and **recording date**

The instrument number is sufficient to:
1. Establish the chain start date
2. Filter out pre-plat document contamination
3. Provide a direct link if visual inspection is needed later

#### Validation Rule: Pre-Plat Document Filtering (Needs Confirmation)

**Status:** Proposed - needs testing on more properties to confirm

**Rule:** Documents recorded BEFORE the plat date with a non-lot legal description should be excluded from the chain of title.

**Example: TOUCHSTONE PHASE 2 (Folio 192935B6Y000008000040U)**

| Instrument | Date | Legal | Grantor → Grantee | Include? |
|------------|------|-------|-------------------|----------|
| 2016416056 | 2016-10-20 | `PT S35 T29 R19` | Busciglio → Lennar | ❌ Parent parcel |
| 2019141481 | 2019-04-04 | PLAT | Lennar (developer) | ✅ ROOT |
| 2019437668 | 2019-10-10 | `L 4 B 8 TOUCHSTONE PH 2` | Lennar → Aponte | ✅ First deed |

**Why this happens:** HCPA links the pre-subdivision deed to the current folio because the folio evolved from that parcel. But the legal description `PT S35 T29 R19` (Section-Township-Range) is the raw land - not the platted lot.

**Detection logic:**
1. Get plat recording date from plat search
2. For any document dated BEFORE plat date:
   - Check if legal description contains lot/block from the plat
   - If legal is Section-Township-Range format (e.g., `PT S35 T29 R19`), exclude
   - If legal doesn't mention the subdivision name, exclude

**TODO:** Test this rule on 10+ properties to confirm it's reliable before implementing.

#### Implementation

**File:** `src/services/step4v2/search_queue.py`
- New method `queue_plat_search()` uses search_type="plat" with format "P:BOOK/PAGE"
- Priority: `PRIORITY_PLAT = 5` (highest - searched before all other types)

**File:** `src/services/step4v2/discovery.py`
- Plat searches pass `book_type="P"` to `search_by_book_page_sync()`

**File:** `config/step4v2.py`
- Added `BOOK_TYPE_PLAT = "P"` constant

### Implementation (Completed 2025-12-26)

**File:** `src/services/step4v2/search_queue.py` - `initialize_for_folio()`

The search queue now follows this priority order:

```python
# TIER 1: EXACT LOOKUPS (No false positives - search these FIRST)
# - 1a. Plat Book/Page from Final Judgment - ROOT OF TITLE
# - 1b. Instrument # from HCPA Sales History (modern recordings)
# - 1c. Book/Page from HCPA Sales History (older recordings)

# TIER 2: HIGH-CONFIDENCE LEGAL DESCRIPTIONS
# - 2a. Final Judgment legal description (most accurate)
# - 2b. HCPA legal description

# TIER 3: LOWER CONFIDENCE SOURCES (Last resort)
# - 3a. Bulk parcel legal descriptions
# - 3b. Generated permutations (wildcards) - HIGH RISK
```

See `search_queue.py` lines 81-156 for the full implementation.

---

## Chain Building Walkthrough (2025-12-26)

### Example Property: TOUCHSTONE PHASE 2 (Folio 192935B6Y000008000040U)

This walkthrough demonstrates the correct search order and filtering logic.

#### Search Priority Order

| Priority | Search Type | Source | Result |
|----------|-------------|--------|--------|
| 5 | Plat | FJ plat_book/page: 135/12 | ✅ Found plat 2019-04-04, Developer: LENNAR |
| 10 | Book/Page | HCPA Sales | None (modern recordings) |
| 15 | Instrument | HCPA: 2016416056 | ❌ Pre-plat parent parcel (exclude) |
| 15 | Instrument | HCPA: 2019437668 | ✅ First deed: LENNAR → APONTE |
| 20 | Case | 292024CA001812A001HC | ⚠️ Returns multiple properties (needs filtering) |
| 30 | Legal | FJ legal_description | Pending |
| 50 | Name | Grantees from deeds | Queue APONTE with date_from=2019-10-10 |

#### Critical Bug: ORI Case Number Index Cross-Contamination (2025-12-26)

The case number search for `292024CA001812A001HC` returned 3 results:

| Row | Plaintiff | Legal | Instrument | Actual Case on Doc |
|-----|-----------|-------|------------|-------------------|
| 1 | LAKEVIEW LOAN SERVICING | (none) | 2025472163 | ✅ 24-CA-001812 |
| 2 | LAKEVIEW LOAN SERVICING | L 4 B 8 TOUCHSTONE PH 2 | 2024090953 | ✅ 24-CA-001812 |
| 3 | REPUBLIC BANK & TRUST CO | #2-16 SWEETWATER TOWNHOMES | 2024090937 | ❌ **24-CA-001809** |

**Investigation Results:**

We opened instrument `2024090937` (the SWEETWATER document) and examined the actual PDF:
- **Case NO. on document:** 24-CA-001809
- **Case we searched for:** 24-CA-001812

These are **completely different case numbers** - only 3 apart (001809 vs 001812).

**Root Cause:** ORI's case number index appears to have a bug where adjacent case numbers filed on the same day (2024-03-04) get cross-indexed. Both Lis Pendens were filed within minutes of each other:
- SWEETWATER (001809): Instrument 2024090937, recorded 11:03:53 AM
- TOUCHSTONE (001812): Instrument 2024090953, recorded same day

**Impact:** Case number searches **cannot be trusted** to return only documents from that case. The ORI database may return documents from neighboring case numbers.

**Required Mitigation - Dual Validation:**

A document from case number search is ACCEPTED if **either** condition is met:
1. **Legal description matches** our property (parsed comparison)
2. **Cross-party matches** - at least one Grantor/Grantee appears in FJ plaintiff/defendant list

| Row | Legal | Grantor/Grantee vs FJ | Result |
|-----|-------|----------------------|--------|
| 1 | (empty) | LAKEVIEW = Plaintiff ✅ | ✅ ACCEPT (party match) |
| 2 | L 4 B 8 TOUCHSTONE | LAKEVIEW = Plaintiff ✅ | ✅ ACCEPT (both match) |
| 3 | SWEETWATER | REPUBLIC BANK ≠ any party | ❌ REJECT (neither matches) |

**Why this works:** Documents in OUR foreclosure case will always involve parties named in the Final Judgment (plaintiff suing defendants). Cross-contaminated documents from OTHER cases will have different parties.

**Edge case:** Prior lienholders (not named in FJ) may record documents referencing our property. These would be caught by legal description match, not party match.

#### Legal Description Fuzzy Matching (2025-12-26)

Legal descriptions for the same property can vary in format:

| Variation | Example |
|-----------|---------|
| Abbreviations | `L 4 B 8` vs `LOT 4 BLOCK 8` |
| Phase format | `PH 2` vs `PHASE 2` vs `PHASE TWO` vs `PH II` |
| Section format | `SEC 20` vs `SECTION 20` |
| Punctuation | `LOT 4, BLOCK 8` vs `LOT 4 BLOCK 8` |

**Matching Algorithm:**

1. **Parse both** legal descriptions using `parse_legal_description()`
2. **Compare components** with different match types:

| Component | Match Type | Example |
|-----------|------------|---------|
| Lot number(s) | **Exact** | `4` = `4` ✅, `4` ≠ `40` ❌ |
| Block number(s) | **Exact** | `8` = `8` ✅, `8` ≠ `18` ❌ |
| Subdivision | **Fuzzy** | `TOUCHSTONE PH 2` ≈ `TOUCHSTONE PHASE 2` ✅ |

3. **Subdivision fuzzy matching:**
   - Normalize abbreviations: PH→PHASE, SEC→SECTION, BLK→BLOCK
   - Normalize numerals: TWO→2, II→2, THREE→3, III→3
   - Calculate fuzzy ratio on normalized strings
   - Threshold: **0.80** (configurable)

**Implementation:** `legal_descriptions_match()` in `src/utils/legal_description.py`

**Match rules:**
- If both have lots → lots must match exactly
- If both have blocks → blocks must match exactly
- If both have subdivisions → fuzzy ratio ≥ threshold
- If one is missing a component the other has → no match on that component (be conservative)

#### Chain of Title (Validated)

| Date | Document | Grantor | Grantee | Legal |
|------|----------|---------|---------|-------|
| 2019-04-04 | PLAT | - | LENNAR HOMES LLC | TOUCHSTONE PH 2 |
| 2019-10-10 | DEED | LENNAR HOMES LLC | APONTE JEANNETTE | L 4 B 8 TOUCHSTONE PH 2 |
| 2024-03-04 | LIS PENDENS | LAKEVIEW LOAN | RODRIGUEZ APONTE | L 4 B 8 TOUCHSTONE PH 2 |
| 2025-11-04 | JUDGMENT | LAKEVIEW LOAN | RODRIGUEZ APONTE | (foreclosure judgment) |

#### Name Search Date Bounds

When processing discovered documents, names are queued with date bounds:

| Document | Party | Role | Search Bounds |
|----------|-------|------|---------------|
| First Deed | LENNAR | Grantor | `date_to=2019-10-10` |
| First Deed | APONTE | Grantee | `date_from=2019-10-10` |

This prevents finding:
- LENNAR documents after they sold (no longer owner)
- APONTE documents before they bought (not yet owner)

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

## Name Variation Linking (Aliases)

**Date Added:** 2025-12-26

### Problem

ORI party names often contain spelling variations, typos, or indexing errors. For example:
- "ROSRIGUEZ APONTE JEANNETTE IVELISSE" (typo in first name)
- "RODRIGUEZ APONTE JEANNETTE IVELISSE" (correct spelling)

Without linking these, chain of title analysis treats them as different people.

### Solution: Cross-Document Name Linking

After discovery completes for a folio, we compare all party names and link spelling variations:

```python
# In DiscoveryEngine._link_party_variations():
for i, name1 in enumerate(party_names):
    for name2 in party_names[i + 1:]:
        match_result = self.name_matcher.match(name1, name2)
        if match_result.is_match and match_result.link_type == "spelling_variation":
            self.name_matcher.detect_and_link(folio, name1, name2)
```

### How Linking Works

1. **NameMatcher.match()** compares two names:
   - Normalizes (uppercase, remove suffixes/titles, sort parts)
   - Calculates fuzzy similarity score (Jaccard on words)
   - Returns `spelling_variation` if score >= 0.85 threshold

2. **detect_and_link()** creates the relationship:
   - Creates `linked_identity` record with canonical name
   - Sets `linked_identity_id` on both `property_parties` rows
   - Logs the link for debugging

3. **Retrieving aliases**:
   ```sql
   SELECT DISTINCT party_name
   FROM property_parties
   WHERE linked_identity_id = ?
   ```

### Link Types Supported

| Type | Description | Confidence |
|------|-------------|------------|
| `exact` | Same after normalization | 1.0 |
| `trust_transfer` | Person <-> Their Trust | 0.9 |
| `spelling_variation` | Typos, misspellings | 0.85+ |

### Spelling Variation Detection Algorithm

The `NameMatcher` class uses a multi-step process to detect spelling variations:

#### Step 1: Normalization

Both names are normalized before comparison:

```python
def normalize(self, name: str) -> str:
    # 1. Uppercase
    name = name.upper().strip()

    # 2. Remove suffixes (JR, SR, II, III, LLC, INC, etc.)
    for suffix in [" JR", " SR", " II", " III", " LLC", " INC", ...]:
        name = name.removesuffix(suffix)

    # 3. Remove titles (MR, MRS, MS, DR, etc.)
    for title in ["MR ", "MRS ", "MS ", "DR ", ...]:
        name = name.removeprefix(title)

    # 4. Remove punctuation
    name = re.sub(r"[.,;:\(\)\[\]\{\}\"\\\/'\`]", "", name)

    # 5. Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()

    # 6. Sort parts alphabetically (handles LAST FIRST vs FIRST LAST)
    parts = sorted(name.split())
    return " ".join(parts)
```

**Example:**
- Input: `"RODRIGUEZ APONTE, JEANNETTE IVELISSE JR."`
- Output: `"APONTE IVELISSE JEANNETTE RODRIGUEZ"`

#### Step 2: Fuzzy Matching (Jaccard Similarity)

If names don't match exactly after normalization, calculate word-level similarity:

```python
def fuzzy_match(self, name1: str, name2: str) -> float:
    n1 = self.normalize(name1)
    n2 = self.normalize(name2)

    if n1 == n2:
        return 1.0

    # Word-level Jaccard similarity
    words1 = set(n1.split())
    words2 = set(n2.split())

    intersection = len(words1 & words2)
    union = len(words1 | words2)
    jaccard = intersection / union

    # Bonus for same word count
    length_bonus = 0.1 if len(words1) == len(words2) else 0.0

    return min(jaccard + length_bonus, 1.0)
```

**Example - Typo Detection:**
- Name 1: `"ROSRIGUEZ APONTE JEANNETTE IVELISSE"` → normalized: `"APONTE IVELISSE JEANNETTE ROSRIGUEZ"`
- Name 2: `"RODRIGUEZ APONTE JEANNETTE IVELISSE"` → normalized: `"APONTE IVELISSE JEANNETTE RODRIGUEZ"`
- Words 1: `{APONTE, IVELISSE, JEANNETTE, ROSRIGUEZ}`
- Words 2: `{APONTE, IVELISSE, JEANNETTE, RODRIGUEZ}`
- Intersection: `{APONTE, IVELISSE, JEANNETTE}` = 3 words
- Union: `{APONTE, IVELISSE, JEANNETTE, ROSRIGUEZ, RODRIGUEZ}` = 5 words
- Jaccard: 3/5 = 0.60
- Length bonus: +0.10 (same word count)
- **Final score: 0.70** (below 0.85 threshold - NOT linked)

**Problem:** Single-letter typos in surnames don't meet the 0.85 threshold with Jaccard alone.

#### Step 3: Enhanced Detection (Future Improvement)

For single-word differences, we should add character-level comparison:

```python
# If only 1 word differs, check if it's a typo (edit distance)
if len(words1 - words2) == 1 and len(words2 - words1) == 1:
    diff1 = list(words1 - words2)[0]  # "ROSRIGUEZ"
    diff2 = list(words2 - words1)[0]  # "RODRIGUEZ"

    # Levenshtein distance / max length
    edit_ratio = 1 - (levenshtein(diff1, diff2) / max(len(diff1), len(diff2)))
    # "ROSRIGUEZ" vs "RODRIGUEZ" = 1 edit / 9 chars = 0.89 similarity

    if edit_ratio >= 0.80:
        return 0.90  # High confidence typo
```

### Implementation Location

- `src/services/step4v2/discovery.py:_link_party_variations()` - Post-discovery scan
- `src/services/step4v2/name_matcher.py:NameMatcher` - Matching logic
- `src/db/migrations/create_v2_database.py` - Schema (`linked_identities`, `property_parties.linked_identity_id`)

---

## Chain of Title Completion Check

**Date Added:** 2025-12-26

### Definition of Complete Chain

A chain of title is **complete** when there is an unbroken sequence of ownership transfers from the **anchor** (root of title) to the **present day**, with no gaps in time.

```
Timeline:
|----------|----------|----------|--------------------------|
Anchor     Deed 1     Deed 2     Deed 3                     Today
(Plat)

Owner:     Developer → Builder → Buyer A → Buyer B ──────────→
           (implied)   (Lennar)   (Rodriguez)   (still owns)
```

### What is the Anchor?

The **anchor** is the root of title - the point at which this specific property came into legal existence:

| Anchor Type | Description | Example |
|-------------|-------------|---------|
| **Subdivision Plat** | Most common for residential lots | Plat Book 135, Page 79 (2019-04-04) |
| **Government Patent** | Original land grant | Federal to private (1800s) |
| **Tax Deed** | Government reclaimed and resold | Tax deed auction sale |
| **Condo Declaration** | Unit created from master parcel | Declaration of Condominium |

For new subdivisions, the plat IS the anchor - the lot didn't exist before the plat was recorded.

### Completion Check Algorithm

```python
def _is_chain_complete(self, folio: str) -> bool:
    """
    Check if chain of title is complete.

    Complete means: from anchor to today, no gap in ownership.

    Steps:
    1. Find anchor (plat date or oldest deed)
    2. Get all deeds sorted by date
    3. Verify chain continuity:
       - First deed is near anchor date
       - Each grantee matches next grantor (fuzzy)
       - Last grantee matches current owner
       - No significant time gaps (> threshold)
    """
```

#### Step 1: Find Anchor Date

```python
# Priority order for anchor:
# 1. Plat document in our documents table
# 2. Plat book/page from HCPA or Final Judgment (look up recording date)
# 3. Oldest deed if no plat (for older properties)

anchor_date = self._get_anchor_date(folio)
if not anchor_date:
    return False  # Can't verify without anchor
```

#### Step 2: Get All Deeds

```python
# Get all ownership transfer documents
deeds = self.conn.execute("""
    SELECT
        recording_date,
        grantor,      -- Party 1 (seller/transferor)
        grantee,      -- Party 2 (buyer/transferee)
        document_type,
        instrument_number
    FROM documents
    WHERE folio = ?
      AND document_type IN ('D', 'DEED', 'WD', 'WARRANTY DEED',
                            'QCD', 'QUIT CLAIM DEED', 'TAXDEED', 'TAX DEED')
    ORDER BY recording_date ASC
""", [folio]).fetchall()
```

#### Step 3: Verify Chain Continuity

```python
# Check 1: First deed should be near anchor
if deeds:
    first_deed_date = deeds[0].recording_date
    days_from_anchor = (first_deed_date - anchor_date).days

    if days_from_anchor > MAX_ANCHOR_GAP_DAYS:  # e.g., 365 days
        return False  # Gap between plat and first deed

# Check 2: Grantee of deed N should match grantor of deed N+1
for i in range(len(deeds) - 1):
    current_grantee = deeds[i].grantee
    next_grantor = deeds[i + 1].grantor

    if not self.name_matcher.match(current_grantee, next_grantor).is_match:
        # Check for linked identity (same person, different spelling)
        if not self._are_linked_identities(current_grantee, next_grantor):
            return False  # Chain broken - different people

# Check 3: Last grantee should be current owner
if deeds:
    last_grantee = deeds[-1].grantee
    current_owner = self._get_current_owner(folio)  # From HCPA/FJ

    if not self.name_matcher.match(last_grantee, current_owner).is_match:
        return False  # Last deed doesn't match current owner

# Check 4: No significant time gaps
for i in range(len(deeds) - 1):
    gap_days = (deeds[i + 1].recording_date - deeds[i].recording_date).days

    if gap_days > MAX_OWNERSHIP_GAP_DAYS:  # e.g., 5 years
        # This might indicate a missing deed
        logger.warning(f"Gap of {gap_days} days between deeds")
        # Don't fail here - gaps can be legitimate (long ownership)

return True  # Chain is complete
```

### Edge Cases

#### New Construction (Builder to First Buyer)

For new subdivisions, the chain often looks like:

```
Plat (2019-04-04)
    ↓ (implied - developer owns all lots after platting)
Developer transfers lot to Builder (may not be recorded separately)
    ↓
Builder (Lennar) sells to First Buyer (Rodriguez Aponte)
    ↓
First Buyer still owns
```

The "developer to builder" transfer might be:
- A bulk deed covering all lots
- Implied (builder IS the developer)
- A separate deed we need to find

#### Multiple Owners (Joint Tenancy)

Deeds may have multiple grantees:
- "JOHN SMITH AND JANE SMITH, HUSBAND AND WIFE"
- "JOHN SMITH AND JANE DOE, AS JOINT TENANTS"

The next deed should have BOTH as grantors, or one if the other died/transferred their interest.

#### Trust Transfers

Person transferring to their own trust:
- "JOHN SMITH" → "JOHN SMITH REVOCABLE TRUST"

This is NOT a gap - it's the same beneficial owner. The `trust_transfer` link type handles this.

### Configuration

```python
# config/step4v2.py

# Maximum days between plat and first deed
MAX_ANCHOR_GAP_DAYS = 730  # 2 years (builders may hold lots)

# Maximum days between consecutive deeds before warning
MAX_OWNERSHIP_GAP_DAYS = 3650  # 10 years (people hold property for decades)

# MRTA fallback - if no plat, chain covering 30 years is marketable
MRTA_YEARS_REQUIRED = 30
```

### Previous (Incorrect) Implementation

The old `_is_chain_complete()` was flawed:

```python
# WRONG - just checks if plat exists, not if chain is unbroken
def _is_chain_complete(self, folio: str) -> bool:
    chain_years = self._calculate_chain_years(folio)
    if chain_years >= MRTA_YEARS_REQUIRED:
        return True

    # This just checks plat EXISTS, not that chain is complete
    result = self.conn.execute("""
        SELECT document_type FROM documents
        WHERE folio = ? AND UPPER(document_type) IN ('PLAT', ...)
    """).fetchone()

    return result is not None  # ← WRONG! Plat is anchor, not proof of complete chain
```

### Implementation Location

- `src/services/step4v2/discovery.py:_is_chain_complete()` - Main check
- `src/services/step4v2/discovery.py:_get_anchor_date()` - Find root of title
- `config/step4v2.py` - Gap thresholds

---

## Instrument Reference Path - Finding Related Documents

**Date Added:** 2025-12-26

### Problem: Legal Search Misses Documents with Variations

Legal description search can miss critical documents due to indexing variations:

| Document | Indexed Legal | Search Term | Match? |
|----------|---------------|-------------|--------|
| Deed | `L 4 B 8 OF TOUCHSTONE PH 2` | `BEGINS L 4 B 8 TOUCHSTONE` | NO |
| Deed | `L 4 B 8 OF TOUCHSTONE PH 2` | `CONTAINS L 4 B 8 TOUCHSTONE` | NO |

The "OF" in the indexed legal breaks both BEGINS and CONTAINS matching.

### Solution: Instrument Reference Path

Documents often reference related instruments in their legal description or cross-reference fields:

```
MODIFICATION document (2023223060):
  Legal: "CLK #2019437669"  ← References original mortgage!
```

When we find an instrument reference:
1. Search for that instrument directly
2. Check adjacent instrument numbers for related documents (deed/mortgage pairs)

### Why Adjacent Instruments Work

When a property is purchased, the deed and mortgage are typically:
- Recorded on the **same day**
- With **sequential instrument numbers**

Example from TOUCHSTONE property:
```
2019437668 = DEED      (Lennar Homes → Rodriguez Aponte)
2019437669 = MORTGAGE  (Rodriguez Aponte → Eagle Home Mortgage)
```

### Instrument Reference Patterns

Documents may contain references in various formats:

| Pattern | Example | Extracted Instrument |
|---------|---------|---------------------|
| `CLK #NNNNNNNNNN` | `CLK #2019437669` | `2019437669` |
| `INST #NNNNNNNNNN` | `INST #2019437669` | `2019437669` |
| `INSTRUMENT NNNNNNNNNN` | `INSTRUMENT 2019437669` | `2019437669` |
| `OR BK NNNNN PG NNNN` | `OR BK 27019 PG 1455` | (book/page lookup) |
| `RECORDED IN OR BOOK` | `RECORDED IN OR BOOK 27019 PAGE 1455` | (book/page lookup) |

### Algorithm: Extract and Search Instrument References

```python
def _extract_instrument_references(self, doc: dict) -> list[str]:
    """
    Extract instrument references from document fields.

    Searches: legal_description, cross_references, notes
    """
    import re

    references = []
    text_fields = [
        doc.get("legal_description", ""),
        doc.get("cross_references", ""),
        doc.get("notes", ""),
    ]

    combined_text = " ".join(str(f) for f in text_fields if f)

    # Pattern: CLK #NNNNNNNNNN or INST #NNNNNNNNNN
    clk_pattern = r'(?:CLK|INST|INSTRUMENT)\s*#?\s*(\d{10})'
    matches = re.findall(clk_pattern, combined_text, re.IGNORECASE)
    references.extend(matches)

    return list(set(references))

def _queue_adjacent_instruments(self, folio: str, instrument: str, doc_type: str):
    """
    When finding a mortgage, check adjacent instruments for the deed.
    When finding a deed, check adjacent instruments for the mortgage.
    """
    inst_num = int(instrument)

    # Check instruments within range of 5 before and after
    for offset in [-5, -4, -3, -2, -1, 1, 2, 3, 4, 5]:
        adjacent = str(inst_num + offset)
        self.search_queue.add_search(
            folio=folio,
            search_type="instrument",
            search_term=adjacent,
            priority=PRIORITY_INSTRUMENT,  # High priority - exact lookup
            triggered_by_instrument=instrument,
        )
```

### When to Use Adjacent Instrument Search

| Found Document | Search Adjacent For |
|----------------|---------------------|
| MORTGAGE (purchase) | DEED (same-day recording) |
| DEED | MORTGAGE (if buyer financed) |
| ASSIGNMENT | Original mortgage/deed |
| MODIFICATION | Original mortgage |
| SATISFACTION | Original mortgage |

### Detecting Purchase Mortgages

A mortgage is likely a "purchase mortgage" if:
1. Recorded near property sale date (from HCPA sales history)
2. Mortgagor matches deed grantee
3. No prior mortgage from same lender on this property

```python
def _is_purchase_mortgage(self, doc: dict, folio: str) -> bool:
    """Check if this mortgage is likely the original purchase mortgage."""
    # Get sales history for this folio
    sale_date = self._get_sale_date_near(folio, doc["recording_date"], days=30)

    if sale_date:
        return True  # Mortgage recorded near a sale = purchase mortgage

    return False
```

### Name Variation Discovery

The instrument path also reveals name variations:

```
Deed grantee:     RODRIGUEZAPONTE JEANNETTE IVELISSE  (no space)
Other documents:  RODRIGUEZ APONTE JEANNETTE IVELISSE (with space)
```

These should be linked via the name matcher as spelling variations.

### Implementation Location

- `src/services/step4v2/discovery.py:_extract_instrument_references()` - Parse references
- `src/services/step4v2/discovery.py:_queue_adjacent_instruments()` - Search nearby instruments
- `config/step4v2.py:ADJACENT_INSTRUMENT_RANGE` - How many to check (default: 5)

---

## Gap-Bounded Searches - Finding Missing Deeds

**Date Added:** 2025-12-26

### Problem: Unbounded Name Searches Return Too Many Results

Name searches like "LENNAR HOMES" can return 6000+ results, exceeding API limits and causing cross-property contamination. We need a way to focus searches on specific time periods where we know deeds are missing.

### Solution: Chain Gap Analysis

After initial discovery exhausts, analyze the chain of title to identify specific date ranges where ownership transfers are missing. Then queue targeted name searches bounded to those date ranges.

### ChainGap Dataclass

```python
@dataclass
class ChainGap:
    """Represents a gap in the chain of title."""

    start_date: date              # When gap starts
    end_date: date                # When gap ends
    gap_type: str                 # Type of gap
    expected_grantor: str | None  # Who should be selling
    expected_grantee: str | None  # Who should be buying
    days: int                     # Number of days in gap
```

### Gap Types

| Gap Type | Description | Example |
|----------|-------------|---------|
| `anchor_to_first_deed` | Gap between plat and first recorded deed | Plat 2019-04-04, first deed 2019-10-10 |
| `ownership_gap` | Break in chain where grantee ≠ next grantor | Smith sells, but Jones (not Smith) is next grantor |
| `to_current_owner` | Last deed grantee doesn't match current owner | Last grantee is Brown, but HCPA shows Davis as owner |

### Algorithm: `_get_chain_gaps()`

```python
def _get_chain_gaps(self, folio: str) -> list[ChainGap]:
    """Identify gaps in the chain of title."""
    gaps = []
    today = datetime.now(tz=UTC).date()

    anchor_date = self._get_anchor_date(folio)
    deeds = self._get_deeds(folio)
    developer = self._get_developer_from_plat(folio)
    current_owner = self._get_current_owner(folio)

    # Gap 1: Anchor to first deed
    if anchor_date and deeds:
        first_deed_date = deeds[0]["recording_date"]
        gap_days = (first_deed_date - anchor_date).days

        if gap_days > MAX_ANCHOR_GAP_DAYS:  # 730 days = 2 years
            gaps.append(ChainGap(
                start_date=anchor_date,
                end_date=first_deed_date,
                gap_type="anchor_to_first_deed",
                expected_grantor=developer,      # Plat developer
                expected_grantee=first_deed.grantor,  # Who owns at first deed
                days=gap_days,
            ))

    # Gap 2: Between consecutive deeds
    for i in range(len(deeds) - 1):
        current_grantee = deeds[i].grantee
        next_grantor = deeds[i + 1].grantor

        if not name_matcher.match(current_grantee, next_grantor).is_match:
            gaps.append(ChainGap(
                start_date=deeds[i].recording_date,
                end_date=deeds[i + 1].recording_date,
                gap_type="ownership_gap",
                expected_grantor=current_grantee,
                expected_grantee=next_grantor,
                days=...,
            ))

    # Gap 3: Last deed to current owner
    if deeds and current_owner:
        last_grantee = deeds[-1].grantee
        if not name_matcher.match(last_grantee, current_owner).is_match:
            gaps.append(ChainGap(
                start_date=deeds[-1].recording_date,
                end_date=today,
                gap_type="to_current_owner",
                expected_grantor=last_grantee,
                expected_grantee=current_owner,
                days=...,
            ))

    return gaps
```

### Example: TOUCHSTONE PHASE 2

```
Plat: 2019-04-04 (Developer: LENNAR HOMES LLC)
First Deed: 2019-10-10 (Grantor: LENNAR → Grantee: RODRIGUEZ APONTE)

Gap Analysis:
- anchor_to_first_deed: 2019-04-04 to 2019-10-10 (189 days)
  - Expected grantor: LENNAR HOMES LLC
  - Expected grantee: ??? (could be builder or first buyer)
```

Since 189 days < MAX_ANCHOR_GAP_DAYS (730 days), this is NOT flagged as a gap - the first deed is close enough to the plat. The developer (Lennar) holding lots for 6 months before selling is normal.

### Algorithm: `_queue_gap_bounded_searches()`

When normal discovery exhausts but chain is incomplete, queue gap-bounded searches:

```python
def _queue_gap_bounded_searches(self, folio: str) -> int:
    """Queue name searches bounded by chain gap dates."""
    gaps = self._get_chain_gaps(folio)
    queued = 0

    for gap in gaps:
        # Queue search for expected grantor (seller) within date range
        if gap.expected_grantor and not is_generic(gap.expected_grantor):
            queue_name_search(
                folio=folio,
                party_name=gap.expected_grantor,
                date_from=gap.start_date,  # BOUNDED!
                date_to=gap.end_date,      # BOUNDED!
                priority=PRIORITY_NAME_CHAIN - 5,  # Higher priority
            )
            queued += 1

        # Queue search for expected grantee (buyer) within date range
        if gap.expected_grantee and not is_generic(gap.expected_grantee):
            queue_name_search(
                folio=folio,
                party_name=gap.expected_grantee,
                date_from=gap.start_date,
                date_to=gap.end_date,
                priority=PRIORITY_NAME_CHAIN - 5,
            )
            queued += 1

    return queued
```

### Impact on Search Results

| Search | Unbounded Results | Bounded Results |
|--------|-------------------|-----------------|
| LENNAR HOMES | 6000+ (exceeds limit) | ~10-50 (within date range) |
| RODRIGUEZ APONTE | 50+ (multiple people) | ~5 (specific date range) |

### Discovery Flow Integration

```python
# In IterativeDiscovery.run():

# Main discovery loop...
while iteration < MAX_ITERATIONS:
    # ... normal discovery ...

# After loop exhausts:
if stopped_reason == "exhausted" and not self._is_chain_complete(folio):
    # Analyze chain for gaps
    gaps_queued = self._queue_gap_bounded_searches(folio)

    if gaps_queued > 0:
        # Continue discovery with gap-bounded searches
        while iteration < MAX_ITERATIONS:
            search = self.search_queue.get_next_ready(folio)
            if not search:
                break

            # Execute gap-bounded search...
```

### Implementation Location

- `src/services/step4v2/discovery.py:ChainGap` - Dataclass for gap representation
- `src/services/step4v2/discovery.py:_get_chain_gaps()` - Detect chain gaps
- `src/services/step4v2/discovery.py:_get_developer_from_plat()` - Get developer name
- `src/services/step4v2/discovery.py:_queue_gap_bounded_searches()` - Queue bounded searches
- `config/step4v2.py:MAX_ANCHOR_GAP_DAYS` - Threshold for anchor-to-first-deed gap (730 days)
- `config/step4v2.py:MAX_OWNERSHIP_GAP_DAYS` - Warning threshold for large gaps (3650 days)

---
