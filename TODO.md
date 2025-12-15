# TODO

## High Priority

### MULTIPLE PARCEL Cases
Properties with "MULTIPLE PARCEL" as their parcel_id cannot be looked up in HCPA, Tax Collector, or other county databases that require a valid folio number. These are typically:
- Foreclosures involving multiple properties bundled together
- Cases where the auction lists several parcels as one sale item

**Challenges:**
- No single folio to query against county systems
- Would need to parse the final judgment PDF to extract individual parcel IDs
- Each parcel would need separate lookups for: HCPA data, tax status, chain of title, permits, etc.
- Database schema may need adjustment to handle one auction -> multiple parcels relationship

**Potential Solutions:**
1. Extract individual parcel IDs from final judgment documents during Step 2
2. Create a junction table `auction_parcels` to link one auction to multiple parcels
3. Run enrichment steps for each parcel independently
4. Aggregate results back to the auction level for analysis

**Current Status:** These properties are skipped by most pipeline steps due to invalid parcel_id.
