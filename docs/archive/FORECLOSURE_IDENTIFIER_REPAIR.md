# Foreclosure Identifier Repair

This note documents a failure mode in the foreclosure hub where a row can have a
non-null `foreclosures.strap` that is not a valid HCPA strap for the same
`folio`.

## Failure mode

- Auction ingestion stores the auction site's `Parcel ID` text directly into
  `foreclosures.strap`.
- Historical reuse can also copy an older strap into a new foreclosure row.
- Before this fix, both the normalize trigger and `src/scripts/refresh_foreclosures.py`
  treated any non-null strap as authoritative.
- If the stored strap was wrong, HCPA-dependent joins silently missed:
  `hcpa_bulk_parcels`, single-pin permits, ORI, market data, and any downstream
  strap-keyed enrichment.

Example observed on February 28, 2026:

- `foreclosures.strap = 0531U2061L000001A`
- `foreclosures.folio = 0774524272`
- HCPA parcel endpoint for `0531U2061L000001A` returned an empty parcel payload
- `hcpa_bulk_parcels` for folio `0774524272` resolved to
  `20310561L000001000360U`

## Repair rule

When both `folio` and `strap` are present:

1. If HCPA has a row where both match, keep the current strap.
2. Else if HCPA has a row for the current strap, keep it.
3. Else if HCPA has a row for the folio, replace `foreclosures.strap` with the
   folio-matched HCPA strap.

This rule is now applied in two places:

- `normalize_foreclosure()` in
  `src/db/migrations/create_foreclosures.py`
- `ENRICH_BASE_SQL` in `src/scripts/refresh_foreclosures.py`

## Operational consequence

Correcting the foreclosure strap can orphan downstream data that was keyed on
the bad strap, especially `ori_encumbrances` and `property_market`. After a
repair, rerun the affected downstream steps for that foreclosure so property-
level data is regenerated on the corrected strap.
