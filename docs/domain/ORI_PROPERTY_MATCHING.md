# ORI Property Text Matching Filter

How `pg_ori_service.py` determines whether a discovered ORI (Official Records Index)
document actually belongs to a given property before storing it as an encumbrance.

See also: [Encumbrance Algorithm](ENCUMBRANCE_ALGORITHM.md) for the upstream
discovery strategy that produces candidate documents.

## Problem Statement

The ORI discovery pipeline uses keyword searches (legal description, party name,
case number, deed-chain adjacency) to find candidate documents in the Hillsborough
County Clerk's PAV (Public Access Viewer) API. These searches return documents that
mention similar terms but may belong to entirely different properties — especially
in subdivisions and condominiums where many units share the same legal description
prefix (e.g., "TOWERS OF CHANNELSIDE CONDOMINIUM").

Without a property-level filter, a condo keyword search for "TOWERS OF CHANNELSIDE"
returns liens for every unit in the building. A subdivision search for "HERITAGE
ISLES" returns documents for every lot in every phase. These get stored as
encumbrances on the wrong property, producing false lien counts and incorrect
equity calculations.

## Architecture

The matching pipeline has two layers, implemented as static methods on
`PgOriService`:

```
Candidate Document (from PAV API)
         │
         ▼
┌─────────────────────────────┐
│  _has_property_text_match() │  ← Legal description matching
│  Compares doc's "Legal"     │
│  field against property     │
│  tokens and locators        │
└─────────┬───────────────────┘
          │ False
          ▼
┌─────────────────────────────┐
│  Party name fuzzy match     │  ← Owner/grantor/grantee matching
│  via rapidfuzz              │
│  token_set_ratio > 80       │
└─────────┬───────────────────┘
          │ False
          ▼
┌─────────────────────────────┐
│  Case number exact match    │  ← LP/JUD only: CaseNum == foreclosure case
└─────────┬───────────────────┘
          │ False
          ▼
      REJECTED
```

A document is accepted if **any** layer returns True. The combination is
implemented in `_matches_property()`.

## Token Generation (`_build_property_tokens`)

For each property, tokens are built from HCPA bulk parcel data:

### Legal Tokens
- Source: `raw_legal1` through `raw_legal4` from `hcpa_bulk_parcels`
- Split on non-alphanumeric characters, keep words >= 3 chars
- **Boilerplate exclusion**: Generic legal vocabulary is removed to prevent
  false matches across unrelated properties that share common terms

**Excluded boilerplate tokens** (`_LEGAL_BOILERPLATE_TOKENS`):
```
CONDOMINIUM, CONDO, CONDOMINIUMS, ASSOCIATION, ASSOC,
UNIT, UNITS, BLOCK, BLK, COMMON, ELEMENTS,
UNDIV, UNDIVIDED, AND, THE, FOR, INT, INC,
INTEREST, TOGETHER, WITH, PLAT, BOOK, PAGE, TRACT,
PHASE, ADDITION, REPLAT, SUBDIVISION, SUBD, SUB,
SECTION, SEC, TOWNSHIP, RANGE, COUNTY, HILLSBOROUGH,
FLORIDA, LIEN, MORTGAGE, SATISFACTION, PARCEL
```

After boilerplate removal, the first 8 words per legal line are kept as tokens.
These are the **distinctive** words that identify the specific property — typically
the subdivision or condo name (e.g., "TOWERS", "CHANNELSIDE", "HERITAGE", "ISLES").

### Legal Locators
- Extracted by `_LEGAL_LOCATOR_RE`: captures `(LOT|UNIT|BLOCK|BLK)\s+(\S+)` pairs
- These are the specific identifiers (e.g., LOT 5, UNIT 2703, BLOCK B)
- Locators are captured **before** boilerplate filtering and stored separately

### Other Token Sets
- **Owner names**: Current owner + all grantees/grantors from ownership chain
- **Street tokens**: From property address, excluding stop words (DR, ST, AVE, etc.)
- **Case number**: From the foreclosure record

## Legal Text Matching (`_has_property_text_match`)

### Word Boundary Matching

All token matching uses `\b{token}\b` regex (word boundary), not substring
containment. This prevents:
- `INT` matching `INTERNATIONAL`
- `AND` matching `BRANDON`
- `LAND` matching `ISLAND`

### NOC Documents (special path)

NOCs (Notices of Commencement) use a more permissive matching strategy because
they typically contain a street address rather than full legal descriptions:

1. If the doc has an explicit street address → match on street number + street name
2. If the property has LOT locators → require LOT match (+ BLOCK if expected)
3. If the property has UNIT locators → require UNIT match
4. Fallback: require >= min(3, token_count) legal token hits

### Non-NOC Documents (primary path)

Non-NOC documents (mortgages, liens, assignments, satisfactions, etc.) use
**locator-first matching** — if the property has specific locators, those must
match before generic token similarity is even considered.

**Priority order:**

1. **LOT match** (checked first): If the property has a LOT locator:
   - LOT value must match in the document
   - If BLOCK is also expected, BLOCK must also match
   - Plus at least 1 legal token hit (subdivision name)
   - *Example: LOT 5 BLOCK B HERITAGE ISLES must match LOT 5 + BLOCK B*

2. **UNIT match** (checked second): If the property has a UNIT locator:
   - UNIT value must match in the document
   - Plus at least 1 legal token hit (condo name)
   - *Example: UNIT 2703 TOWERS OF CHANNELSIDE must match UNIT 2703*
   - This prevents a mortgage on UNIT 1105 from matching a property with UNIT 2703

3. **Street address fallback**: If no locators but street tokens match >=
   min(2, token_count), accept the document

4. **Generic token fallback**: If no locators and no street match, require
   ~40% of legal tokens to match:
   ```
   min_required = max(2, (len(legal_tokens) * 2 + 4) // 5)
   ```
   This scales with token count — a property with 10 distinctive tokens requires
   4 matches, not just 2.

## Why This Ordering Matters

LOT is checked before UNIT because subdivision properties may have both a UNIT
(phase number) and LOT/BLOCK (specific parcel). In that case, LOT/BLOCK is the
more specific identifier:

```
Legal: "LOT 5 BLOCK B HERITAGE ISLES PH 1C UNIT 1"
                                          ↑ phase, not apartment
```

If UNIT were checked first, it would match any document mentioning "UNIT 1" in
any Heritage Isles phase — which is thousands of properties. LOT 5 + BLOCK B
narrows it to exactly one.

## Data Quality Fix (2026-03-05)

### Root Cause

Prior to this fix, three compounding bugs allowed unrelated documents to pass
the property filter:

1. **Substring matching**: Token matching used `t in text` (Python `in` operator)
   instead of word-boundary regex. This caused `INT` to match `INTERNATIONAL`,
   `AND` to match `BRANDON`, etc. — producing false positives across
   unrelated properties.

2. **Generic vocabulary inflation**: Tokens like `CONDOMINIUM`, `UNIT`, `AND`,
   `COMMON`, `ELEMENTS` were included in the legal token set. Since every condo
   document contains these words, they inflated the match count without
   providing any discriminating signal.

3. **Low threshold**: The match threshold was `min(2, len(legal_tokens))`. With
   10 tokens (most of which were generic), only 2 hits were needed — trivially
   satisfied by any condo document containing `CONDOMINIUM` + `UNIT`.

### Impact

65 ORI instruments were "sprayed" across 5+ unrelated properties, creating
175+ bad encumbrance rows across 45 straps (28% of active properties). The most
visible symptom was the Towers of Channelside property (strap
`1929199ERT00001027030A`) showing liens from Villa Sonoma, Tudor Cay, North Oaks,
River Oaks, Grand Reserve, Sunridge, and 10+ other unrelated condo associations.

### Fix Applied

1. **Boilerplate token exclusion**: Added `_LEGAL_BOILERPLATE_TOKENS` frozenset
   (~40 common terms) and filtered them from `_build_property_tokens()`.
   After filtering, only distinctive words (subdivision/condo proper names)
   remain as matching tokens.

2. **Word-boundary matching**: Replaced `sum(1 for t in tokens if t in text)`
   with `_word_boundary_hits()` using `re.search(rf"\b{re.escape(t)}\b", text)`.

3. **Locator-first matching**: Added LOT and UNIT locator requirements for
   non-NOC documents. If the property has a UNIT locator, the document must
   contain the same UNIT number — regardless of how many generic tokens match.

4. **Scaled threshold**: Changed fallback from `min(2, token_count)` to
   `max(2, (token_count * 2 + 4) // 5)` (~40% of distinctive tokens).

### Validation

Tested against all 124 active foreclosures with full ownership chain data:

| Metric | Value |
|--------|-------|
| Total encumbrances tested | 1,418 |
| Accepted by new filter | 1,020 (72%) |
| Rejected by new filter | 398 (28%) |
| False negatives found | 0 |
| Bad rows deleted | 384 |

All 60 rejected LP/JUD encumbrances were manually verified as belonging to
different properties (wrong lots, wrong units, wrong subdivisions). The
Towers of Channelside property went from 37 encumbrances to 13, with all
wrong condo associations removed.

## Future Considerations

- **BLK vs BLOCK inconsistency**: The locator regex captures both `BLOCK` and
  `BLK`, but the match check currently looks for them as separate keys. A
  document with "BLK 5" will match a property with "BLK 5" but not one with
  "BLOCK 5". This is a pre-existing edge case that affects a small number of
  subdivision properties.

- **Reference chain documents**: Assignments, releases, and satisfactions
  sometimes have minimal legal text (e.g., "CLK# 2025146069 TERMINATION").
  These are currently matched via party name or not at all. A future
  improvement could match them by checking if they reference an instrument
  number that belongs to an already-accepted encumbrance.
