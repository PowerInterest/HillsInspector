# Design 1: Encumbrance Extraction Repair Prompt

## Goal

When an encumbrance extraction fails to resolve a property address or strap against HCPA data, fire a targeted repair prompt that tells the LLM what went wrong and provides Hillsborough County zip codes as a hard constraint. The existing extraction prompts stay untouched.

## Problem

~5% of encumbrance extractions produce addresses or party names that can't be matched to HCPA parcels. Root causes observed:

| Error | Example | Root Cause |
|-------|---------|------------|
| Extracted lender's address instead of property | `951 Yamato Road, Boca Raton` (Freedom Mortgage office) | LLM picked the "Return To" header address |
| OCR-garbled address | `40 Bae Lin Cont. Ta. aida` instead of `4010 Pine Limb Court, Tampa, FL 33614` | LLM picked a garbled stamp over clean body text |
| Null core fields | Consensual lien with null lienee/lienor despite clear parties in text | LLM couldn't map unusual doc type to schema fields |
| Wrong date | Recording date `1997-06-13` (form print date) instead of `1999-08-03` (clerk stamp) | Multiple dates on page, LLM picked wrong one |

## Architecture

Add a repair pass to `_process_one()` in `pg_encumbrance_extraction_service.py` that triggers **after** initial extraction + validation succeeds but the extracted `property_address` fails to match any HCPA parcel. The repair prompt includes:

1. The original OCR text (already in `raw` column or in memory)
2. The original extraction result (what the LLM returned)
3. Specific error feedback ("address X is not in Hillsborough County")
4. The full list of valid Hillsborough County zip codes
5. Disambiguation rules for common mistakes

### Flow

```
PDF → OCR → save raw → LLM (existing prompt) → validate → extracted_data
                                                              ↓
                                                    HCPA address lookup
                                                              ↓
                                                     Match? → Done
                                                     No match? → Repair prompt
                                                              ↓
                                                     LLM (repair prompt) → validate
                                                              ↓
                                                     Save best result
```

### Where It Runs

The repair check runs inside `_process_one()`, after validation succeeds (line ~945) and before the cache write (line ~948). It does NOT run for cache hits — only fresh extractions.

### HCPA Lookup

A lightweight check: normalize the extracted `property_address` and attempt to match against `hcpa_bulk_parcels.property_address`. If zero matches, trigger the repair prompt. The lookup is a simple SQL query — no fuzzy matching needed at this stage since the repair prompt will produce a better address.

```python
def _address_resolves(self, address: str | None) -> bool:
    """Check if extracted address matches any HCPA parcel."""
    if not address or len(address) < 5:
        return False
    normalized = address.upper().strip().split(",")[0]
    sql = text("""
        SELECT 1 FROM hcpa_bulk_parcels
        WHERE property_address = :addr
        LIMIT 1
    """)
    with self.engine.connect() as conn:
        return conn.execute(sql, {"addr": normalized}).fetchone() is not None
```

### Repair Prompt

```
You previously extracted data from this Hillsborough County document, but the
property address you returned does not match any known parcel in the county.

YOUR PREVIOUS EXTRACTION:
{json.dumps(original_extraction, indent=2)}

THE ERROR:
{error_description}

VALID HILLSBOROUGH COUNTY ZIP CODES:
33503, 33510, 33511, 33527, 33534, 33544, 33547, 33548, 33549,
33556, 33558, 33559, 33563, 33565, 33566, 33567, 33569, 33570,
33572, 33573, 33575, 33578, 33579, 33584, 33592, 33594, 33596,
33598, 33601, 33602, 33603, 33604, 33605, 33606, 33607, 33609,
33610, 33611, 33612, 33613, 33614, 33615, 33616, 33617, 33618,
33619, 33624, 33625, 33626, 33629, 33634, 33635, 33636, 33637,
33647

COMMON MISTAKES TO CORRECT:
- The property address is in the GRANTING CLAUSE or LEGAL DESCRIPTION section,
  NOT the "Return To" / "Prepared By" / "After Recording" header block
- If the zip code is not in the list above, you have the WRONG address
  (likely the lender's, attorney's, or servicer's office)
- The BORROWER/MORTGAGOR grants the mortgage — the return-to contact is NOT
  the borrower
- The RECORDING DATE is on the clerk's stamp (top-right, "RECORDED" or "INSTR #"),
  not the form print date at the bottom of the page
- For UCC/Consensual Liens: always extract parties and amounts even if the
  document type is unusual — secured party = creditor, debtor = lienee

ORIGINAL OCR TEXT:
{ocr_text}

Return a corrected JSON object with the same schema. Fix the property_address
and any other fields that were wrong. Use null only if the information truly
does not appear anywhere in the document.
```

### Error Description Generation

The `error_description` is constructed based on what failed:

- Address has non-Hillsborough zip: `"You extracted address '{addr}' with zip code {zip}, which is not in Hillsborough County. This is likely the lender's or attorney's office address."`
- Address is null: `"No property address was extracted. Look for it in the granting clause, legal description, or sale paragraph."`
- Address doesn't match HCPA: `"Address '{addr}' does not match any known parcel. Check for OCR errors in the street name or number."`

### Integration Points

- **`_process_one()`**: After validation, before cache write. One repair attempt max.
- **`_extract_from_ocr_text()`**: Reused for the repair call (same LLM endpoint).
- **`raw` column**: OCR text is already persisted by this point, but kept in memory for the repair prompt.
- **Cache**: Only the final (repaired or original) result is cached.
- **Logging**: Log when repair is triggered and whether it improved the result.

### What It Does NOT Do

- Does not modify existing prompts
- Does not run on cache hits
- Does not retry more than once
- Does not do fuzzy address matching — that's the identifier recovery service's job
- Does not block extraction if repair also fails — saves the best available result

## Files to Modify

- `src/services/pg_encumbrance_extraction_service.py`: Add `_address_resolves()`, repair prompt constant, repair logic in `_process_one()`
- `src/services/vision_service.py`: No changes
- `tests/test_encumbrance_extraction_service.py`: Add tests for repair flow

## Success Criteria

- The 5 encumbrances we identified with wrong/missing addresses (140408, 138560, 136953, 137024, 139306) should produce better results on re-extraction with repair
- No regression on the 90%+ that already extract correctly (repair never fires for them)
- Repair adds at most ~2 seconds per affected document (one extra LLM call)
