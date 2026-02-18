# Hillsborough Bulk Data Reference

## Data Fields

The parcel shapefile contains polygons. A shapefile is a series of files used to draw geographic data in GIS software. One component, `parcel_mm_dd_yyyy.dbf`, is a table of values.

The following fields are available in the `.dbf` table for current properties in Hillsborough County:

| Field | Description |
| --- | --- |
| `TYPE` | Description of parcel, especially if parcel has no folio number. |
| `FOLIO` | Parcel folio number (text field, full 10-digit folio number; no decimals or dashes). |
| `ACREAGE` | Area (calculated from the polygon, not based on the deed). |
| `PIN` | Property identification number (see structure section below). |
| `DOR_C` | DOR property use code (see `parcel_dor_names.dbf`); primary use of parcel. |
| `OWNER` | Owner name. |
| `ADDR_1` | Owner mail address 1. |
| `ADDR_2` | Owner mail address 2. |
| `CITY` | Owner mail city. |
| `STATE` | Owner mail state. |
| `ZIP` | Owner mail zip. |
| `COUNTRY` | Owner mail country. |
| `SUB` | 3-character subdivision code (see `parcel_sub_names.dbf`). |
| `SITE_ADDR` | Parcel site address (if available). For multi-building parcels only one site address is stored. Notes from source: addresses like `0 Kennedy Blvd` may appear and may be unsuitable for mailing; some vacant-parcel addresses may be incomplete/inaccurate; consider DOR code when using for mail operations. |
| `SITE_CITY` | Parcel site city (`Tampa`, `Plant City`, `Temple Terrace`, or `Unincorporated`). |
| `SITE_ZIP` | Parcel site zip. |
| `LEGAL1` | Legal description line 1 (max 50 chars). |
| `LEGAL2` | Legal description line 2 (max 50 chars). |
| `LEGAL3` | Legal description line 3 (max 50 chars). |
| `LEGAL4` | Legal description line 4 (max 50 chars). |
| `NBHC` | Neighborhood code. |
| `TAXDIST` | Millage/tax district. |
| `JUST` | Market/just value (for non-greenbelt parcels, also assessed value). |
| `LAND` | Land value. |
| `BLDG` | Total building value on parcel. |
| `EXF` | Value of extra features (fences, dock, shed, pool, etc.). |
| `ACT` | Actual year built (if multiple buildings, building #1). |
| `EFF` | Year used for depreciation calculations. |
| `HEAT_AR` | Heated sq ft (living area) of all buildings on parcel. |
| `ASD_VAL` | Assessed value (land + improvements less applicable caps). Different taxing authorities may calculate differently. |
| `TAX_VAL` | Taxable value (`asd_val` less exemptions). Not applicable to school taxes because it includes reduction for additional $25,000 homestead exemption. |
| `SD1` | Special tax district. |
| `SD2` | Special tax district. |
| `TIF` | Tax increment fund. |
| `MUNI` | Municipality code: `A=Tampa`, `U=Unincorporated`, `T=Temple Terrace`, `P=Plant City`. |
| `S_DATE` | Sale date (qualified free-market sales only, since 1906). |
| `VI` | Vacant or improved at time of sale. |
| `AMT` | Sale amount (qualified sales only). |
| `STRAP` | Unformatted PIN number; use for linking shapefile to MAF data files. |
| `DBA` | Doing Business As (business name). |
| `tBEDS` | Total bedrooms for all buildings on parcel. |
| `tBATHS` | Total bathrooms for all buildings on parcel. |
| `tUNITS` | Total living units for all buildings on parcel. |
| `tSTORIES` | Sum of stories across all buildings on parcel. |
| `tBLDGS` | Total number of buildings on parcel. |
| `BASE` | Year homestead approved (residential) or cap applied (non-residential); `0` means no homestead/cap. |
| `Edit_dt` | Date mapping polygon was last updated. |

## Also Included in `parcel_*.zip`

- `parcel_dor_names.dbf`: FL DOR property use codes and descriptions.
- `parcel_sub_names.dbf`: subdivision codes and names.

Additional source notes:

- Multistory condo units are included as small 1-foot square polygons.
- Some parcel types (water, rights-of-way, closed rights-of-way, landscape tracts, parks, etc.) may not always be included.
- Files contain polygons only (no arc attributes), and are intended for parcel analytical purposes.

## Parcel Package Variants (Verified)

Two similarly named parcel packages are published in the root folder and they are not identical.

### `parcel_02_13_2026.zip`

- Main table: `parcel.dbf`
- Rows: `530,315` (`530,314` non-empty folio)
- Columns: `47`
- Uses `DOR_C` field name
- Includes `parcel.shp.xml` metadata sidecar

### `HCparcel_4_public_02_13_2026.zip`

- Main table: `parcel_4_public.dbf`
- Rows: `530,324`
- Columns: `55`
- Uses `DOR_CODE` field name
- Adds additional attributes useful for ETL/normalization:
  - `MARKET_VAL`
  - `LU_GRP`
  - parsed street fields: `str`, `str_pfx`, `str_sfx`, `str_unit`, `str_num`
  - `FOLIO_NUMB`

### Observed overlap/delta notes

- Folio sets are close but not identical:
  - folios in `HCparcel_4_public` not in `parcel`: `19`
  - folios in `parcel` not in `HCparcel_4_public`: `9`
- Treat these as two snapshots from related but not strictly identical publish pipelines.

## Sidecar Files In Parcel Zips

### `parcel_dor_names.dbf`

- Rows: `305`
- Columns: `DORCODE`, `DORDESCR`
- Use: authoritative decode table for parcel use codes (`DOR_C` / `DOR_CODE`).

### `parcel_sub_names.dbf`

- Rows: `11,492`
- Columns: `SUBCODE`, `SUBNAME`, `PLAT_BK`, `PAGE`
- Use: decode table for `SUB` and subdivision/plat normalization.

### GIS sidecars

- `*.shp`, `*.shx`, `*.sbn`, `*.sbx`, `*.prj`, `*.cpg` are geometry/projection/index support files.
- Useful for GIS tools; not required for tabular-only PostgreSQL ingest.
- `parcel.shp.xml` (when present) is metadata, not transactional parcel data.

## Weekly Download Strategy

Recommended for the PostgreSQL pipeline:

1. Download and load `allsales_*.zip` every week.
2. Download and load one parcel snapshot every week.
3. Prefer `HCparcel_4_public_*.zip` as primary parcel source because of richer columns.
4. Optionally fetch `parcel_*.zip` for reconciliation/audit checks, but not required for routine weekly updates.
5. Refresh lookup files (`parcel_dor_names.dbf`, `parcel_sub_names.dbf`, `subdivisions_*.zip`, `special_districts_*.zip`) only when hash/date changes.

## Sales Notes

- Sales are posted after receipt/review from the Clerk's Office.
- More sales data is available in `allsales_MM_DD_YYYY.zip`.
- For faster/custom data delivery, source mentions Clerk custom email options.
- For more complete data, source references MAF/assessment CD contact:
  - Marilyn Martinez, `813-276-8810`, `martinezm@hcpafl.org`.

## Condos / Multi-Owner Properties

For multi-story and other multi-owner properties where interior plats are unavailable:

- A 1-foot square polygon is drawn for each unit/owner (including common areas).
- Each polygon label point contains the unit/owner 10-digit `FOLIO`.

Example (Atrium on the Bayshore pattern):

- A condo "header" record is created in CAMA with `dor code = HH`.
- Unit records are created with `dor code = 0400`.
- Folio numbers often increment by `.0002` per unit.

| Folio | DOR code | Sub code | Owner |
| --- | --- | --- | --- |
| `1174807200` | `HH` | `3P3` | XXXX Atrium on the Bayshore a condominium |
| `1174807202` | `0400` | `3P3` | Smith, Joe |
| `1174807204` | `0400` | `3P3` | Smyth, Jane |
| `...` | `...` | `...` | `...` |
| `1174807452` | `0400` | `3P3` | Smythe, Jane |
| `1174810000` | `HH` | `3P4` | XXXX Bay Villa |

Implementation implication:

- Header polygon represents overall legal boundary (e.g., `1174807200`).
- Unit polygons are small and may be randomly placed inside/near header polygon due to space constraints.

## STRAP and PIN Number Structure

Use `STRAP` to join shapefile records to other HCPA data files.

Example STRAP:

```text
2819163A3000034000040A
28  19  16  3A3  000034  000040  A
```

Breakdown:

| Part | Meaning |
| --- | --- |
| `28` | Township |
| `19` | Range |
| `16` | Section |
| `3A3` | Land Type-ID |
| `000034` | Block # |
| `000040` | Lot # |
| `A` | Municipality |

PIN is a formatted representation of STRAP.

Example PIN:

```text
A-16-28-19-3A3-000034-00004.0
```

| PIN Segment | Meaning |
| --- | --- |
| `A` | Municipality |
| `16` | Section |
| `28` | Township |
| `19` | Range |
| `3A3` | Land Type-ID |
| `000034` | Block # |
| `00004.0` | Lot # |

Municipality codes:

- `A`: City of Tampa
- `T`: City of Temple Terrace
- `P`: City of Plant City
- `U`: Unincorporated county

Land Type-ID notes:

- Platted subdivisions/condos have assigned IDs (e.g., `3A3`).
- Unplatted/metes-and-bounds lands use `ZZZ`.

Block/Lot notes:

- `BLOCK#` is 6 digits: block number for platted subdivisions, building number for condos, unique assigned number for `ZZZ`.
- `LOT#` is 6 digits: lot number for subdivisions, unit number for condos, unique assigned number for `ZZZ`.
