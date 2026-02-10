# Step 3: Bulk Data Enrichment

## Overview
This step enriches the `parcels` table using the comprehensive bulk data dump from the Hillsborough County Property Appraiser (HCPA). This provides the "source of truth" for ~528,000 parcels, including verified owners, addresses, and assessed values.

## Source
- **Source**: Local DBF/Parquet files
- **Origin**: [HCPA Downloads](https://www.hcpafl.org/Downloads/GIS)
  - `parcel_MM_DD_YYYY.zip` (Main characteristics)
  - `LatLon_Table_MM_DD_YYYY.zip` (Geographic coordinates)
- **Method**: Polars

## Process Flow

1.  **Ingestion**:
    - The `bulk_parcel_ingest.py` script reads the `parcel.dbf` file using `dbfread`.
    - It converts the data to a Polars DataFrame for high-performance processing.
    - The data is saved to a Parquet file (`data/parquet/bulk_parcels_latest.parquet`) for fast future access.
    - The data is bulk-inserted into the `bulk_parcels` table in `data/property_master.db`.

2.  **Enrichment**:
    - The script identifies auctions in the `auctions` table that lack parcel details.
    - It joins `auctions` with `bulk_parcels` on the **STRAP** (State Tax Route Area Parcel) number.
    - It upserts records into the `parcels` table.

## Data Points

The following key fields are populated in the `parcels` table:

- **Identity**: `owner_name`, `property_address`, `city`, `zip_code`
- **Property Details**: `year_built`, `beds`, `baths`, `heated_area`, `lot_size`, `land_use`
- **Value**: `assessed_value`, `market_value`, `just_value`
- **Coordinates**: `latitude`, `longitude` (Sourced from `LatLon_Table`)
- **Legal Description**: `legal_description` (Constructed from `raw_legal1`...`raw_legal4`)

## Importance
This step is critical because:
1.  Auction data often has incomplete or malformatted addresses.
2.  The legal description from bulk data is needed to search for title documents (Step 5).
3.  Assessed value provides a baseline for equity analysis.

## Maintenance
The bulk data file should be refreshed **weekly** (HCPA updates it on Sundays).
Run: `uv run python -m src.ingest.bulk_parcel_ingest --download` to auto-fetch the latest files.

> [!NOTE]
> **Optimization**: By enriching `latitude` and `longitude` here, the pipeline avoids expensive calls to the external geocoding API in Step 15 for the majority of properties.
