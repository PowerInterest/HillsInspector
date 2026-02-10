# Step 2.5: Resolve Missing Parcel IDs

## Purpose
Resolve missing `auctions.parcel_id` values after judgment extraction so downstream steps (HCPA, ORI, permits, survival) can run. This step uses judgment data and bulk parcel data to map each auction to a valid strap/folio.

## Placement In Pipeline
Runs **after Step 2 (Final Judgment Extraction)** and **before Step 3 (Bulk Enrichment)**.

## Inputs
From SQLite:
1. `auctions` rows with empty `parcel_id` and `extracted_judgment_data` present.
2. `auctions.property_address` for cases without judgment data but with a scraped address.
3. `bulk_parcels` for address lookup and disambiguation.

From judgment extraction (`extracted_judgment_data` JSON):
1. `property_address`
2. `parcel_id` in clerk format, e.g. `A-08-29-19-4NU-B00000-00004.0`
3. `legal_description` and parsed fields (unit, lot, block, subdivision, is_condo)
4. `defendants`

## Resolution Chain (Reliability Order)
1. **Judgment parcel_id to strap conversion** (deterministic)
2. **Exact address match** against `bulk_parcels.property_address` (unique match)
3. **Legal description disambiguation** using `raw_legal1-4`
4. **Defendant name matching** against `bulk_parcels.owner_name`

## Decision Rules
1. **Immediate skips**
   - Skip if `auctions.folio` is `MULTIPLE PARCEL`.
   - Skip if no judgment data and no auction address.
   - Skip if `parcel_id` already present.

2. **Strategy 1: Judgment parcel_id to strap conversion**
   - Convert clerk format to strap format using the known encoding (Range + Township + Section + Subdivision + Block + Lot + Qualifier + Suffix).
   - **Accept only if the converted strap exists in `bulk_parcels.strap`.** If `bulk_parcels` is empty, this strategy will not resolve anything.
   - If not found, try alternate suffix (e.g., `U` for condos).

3. **Strategy 2: Exact address match**
   - Normalize address to the street line only (before the comma), uppercase. If the judgment address lacks a comma and includes city/state, it may not match until standardized.
   - Query `bulk_parcels.property_address` for an exact match.
   - If exactly one match is found, accept it.
   - If more than one match is found, move to Strategy 3.
   - If zero matches, move to Strategy 1 or Strategy 3 depending on available data.

4. **Strategy 3: Legal description disambiguation**
   - Concatenate `raw_legal1`, `raw_legal2`, `raw_legal3`, `raw_legal4` into a single search string per candidate.
   - Try to match judgment fields in this order: unit, lot, block, subdivision.
   - If exactly one candidate has the best score and score is at least 1, accept it.
   - If still ambiguous, move to Strategy 4.
   - If there is no judgment data, Strategy 3 is skipped (no structured fields to use).

5. **Strategy 4: Defendant name matching**
   - Normalize each defendant name: uppercase, strip legal suffixes (LLC, INC, CORP, ET AL, TRUSTEE, AS TRUSTEE, A/K/A).
   - Build a set of significant words (3+ chars, exclude AND, THE, OF, FOR).
   - Match candidates where all significant words appear in `bulk_parcels.owner_name`.
   - Accept only if exactly one candidate matches.
   - If there is no judgment data, Strategy 4 is skipped (no defendants available).

## Updates
When resolved, update `auctions`:
```
UPDATE auctions
SET folio = ?, parcel_id = ?, has_valid_parcel_id = 1, updated_at = CURRENT_TIMESTAMP
WHERE case_number = ?
```

No updates are made to `parcels` in this step. HCPA enrichment will populate `parcels` after the parcel_id is resolved.

## Logging Requirements
Every decision must be logged so a reviewer can follow the exact reasoning.

Example log:
```
[RESOLVE] === 292016CA004539A001HC ===
[RESOLVE] Status: no parcel_id, has_valid_parcel_id=1
[RESOLVE] Data sources: judgment=NO, auction_address="15610 HOWELL PARK LN, TAMPA, FL- 33625"
[RESOLVE] Strategy 1: SKIP — no judgment data available
[RESOLVE] Strategy 2: using auction address
[RESOLVE] Strategy 2: address_exact query="15610 HOWELL PARK LN" -> 1 result
[RESOLVE]   candidate: strap=1827319C3000001000120U, owner="FRANCIS HORNE/TRUSTEE"
[RESOLVE] RESOLVED via address_exact_unique -> strap=1827319C3000001000120U
```

## Known Edge Cases
1. **Multiple units at one address** (condos or multifamily)
   - Address matches are ambiguous without unit or lot/block information.
   - `raw_legal2` often contains unit identifiers, but unit can appear in any `raw_legal1-4` field.

2. **Owner name mismatch**
   - The HCPA owner may be a later buyer, not the judgment defendant.
   - Name matching is treated as the last-resort strategy.

3. **Bulk data not loaded**
   - If `bulk_parcels` is empty, **none** of the strategies can resolve (including strap conversion, which validates against bulk).
   - Address and legal description matching require bulk data.

## Output Metrics
Return a summary with counts:
```
{
  "total_candidates": N,
  "resolved": M,
  "skipped_no_data": X,
  "skipped_multiple_parcel": Y,
  "skipped_ambiguous": Z,
  "by_strategy": {
    "strap_conversion": a,
    "address_exact_unique": b,
    "legal_desc_match": c,
    "name_match": d
  }
}
```

## Notes
- Address match hit rate on the initial sample was 5/6, but this is not guaranteed for condos or multi-family properties.
- The resolution order favors deterministic identifiers over fuzzy matching.
