"""
Bulk parcel data ingestion from HCPA shapefile/DBF.

Downloads and processes the weekly parcel data dump from Hillsborough County
Property Appraiser. This provides the "source of truth" for ~480,000 parcels.

The PG-native bulk loader lives in ``sunbiz/pg_loader.py`` (``load_hcpa_suite``).
This module provides the low-level DBF→Polars conversion helpers that
``pg_loader`` depends on (``dbf_to_polars``, ``load_latlon_data``), plus
Parquet serialisation utilities for offline analysis.

IMPORTANT: Uses Polars (NOT Pandas) and Parquet for efficient storage.

Data Source:
    https://www.hcpafl.org -> DOWNLOADS -> GIS DATA
    File: parcel_MM_DD_YYYY.zip (contains parcel.dbf)
    Updated: Weekly (Sundays) - we only refresh weekly
"""

import polars as pl
import tempfile
import zipfile
import json
from pathlib import Path
from loguru import logger
from src.utils.time import coerce_datetime_utc, now_utc
import asyncio
from src.ingest.bulk_downloader import download_latest_bulk_data

# For reading DBF files (shapefile attribute table)
try:
    from dbfread import DBF
    DBF_AVAILABLE = True
except ImportError:
    DBF = None  # type: ignore[assignment,misc]
    DBF_AVAILABLE = False
    logger.warning("dbfread not installed. Run: uv add dbfread")


# Paths
DATA_DIR = Path("data")
BULK_DATA_DIR = DATA_DIR / "bulk_data"
PARQUET_DIR = DATA_DIR / "parquet"
METADATA_FILE = BULK_DATA_DIR / "ingest_metadata.json"

# HCPA download URL pattern
HCPA_DOWNLOAD_BASE = "https://www.hcpafl.org/Downloads/GIS"

# Refresh interval (only re-download if older than this)
REFRESH_INTERVAL_DAYS = 7


# Column mapping from DBF to our schema
COLUMN_MAPPING = {
    "FOLIO": "folio",
    "PIN": "pin",
    "STRAP": "strap",
    "OWNER": "owner_name",
    "SITE_ADDR": "property_address",
    "SITE_CITY": "city",
    "SITE_ZIP": "zip_code",
    "DOR_CODE": "land_use",
    "LU_GRP": "land_use_desc",
    "ACT": "year_built",
    "tBEDS": "beds",
    "tBATHS": "baths",
    "tSTORIES": "stories",
    "tUNITS": "units",
    "tBLDGS": "buildings",
    "HEAT_AR": "heated_area",
    "ACREAGE": "lot_size",
    "ASD_VAL": "assessed_value",
    "MARKET_VAL": "market_value",
    "JUST": "just_value",
    "LAND": "land_value",
    "BLDG": "building_value",
    "EXF": "extra_features_value",
    "TAX_VAL": "taxable_value",
    "S_DATE": "last_sale_date",
    "S_AMT": "last_sale_price",
    "TYPE": "raw_type",
    "SUB": "raw_sub",
    "TAXDIST": "raw_taxdist",
    "MUNI": "raw_muni",
    "LEGAL1": "raw_legal1",
    "LEGAL2": "raw_legal2",
    "LEGAL3": "raw_legal3",
    "LEGAL4": "raw_legal4",
}


def ensure_directories():
    """Create necessary directories."""
    BULK_DATA_DIR.mkdir(parents=True, exist_ok=True)
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)


def get_ingest_metadata() -> dict:
    """Load metadata about previous ingestions."""
    if METADATA_FILE.exists():
        with open(METADATA_FILE) as f:
            return json.load(f)
    return {}


def save_ingest_metadata(metadata: dict):
    """Save ingestion metadata."""
    with open(METADATA_FILE, "w") as f:
        json.dump(metadata, f, indent=2, default=str)


def should_refresh() -> bool:
    """
    Check if we should refresh the bulk data.

    Returns True if:
    - No previous ingestion
    - Last ingestion was more than REFRESH_INTERVAL_DAYS ago
    """
    metadata = get_ingest_metadata()
    last_ingest = metadata.get("last_ingest_date")

    if not last_ingest:
        logger.info("No previous ingestion found - will download fresh data")
        return True

    last_date = coerce_datetime_utc(last_ingest)
    if not last_date:
        logger.info("Invalid last_ingest_date - will download fresh data")
        return True
    days_since = (now_utc() - last_date).days

    if days_since >= REFRESH_INTERVAL_DAYS:
        logger.info(f"Last ingestion was {days_since} days ago - will refresh")
        return True

    logger.info(f"Last ingestion was {days_since} days ago - skipping (threshold: {REFRESH_INTERVAL_DAYS} days)")
    return False


def safe_str(value) -> str | None:
    """Safely convert value to string or None."""
    if value is None:
        return None
    if isinstance(value, str):
        s = value.strip()
        return s if s else None
    return str(value)


def safe_float(value) -> float | None:
    """Safely convert value to float or None."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip()) if value.strip() else None
        except ValueError:
            return None
    return None


def safe_date(value) -> str | None:
    """Safely convert date to ISO string or None."""
    if value is None:
        return None
    if hasattr(value, 'isoformat'):
        return value.isoformat()
    if isinstance(value, str):
        return value.strip() if value.strip() else None
    return None


def dbf_to_polars(dbf_path: Path) -> pl.DataFrame:
    """
    Convert DBF file to Polars DataFrame efficiently.

    Uses dbfread to iterate and builds a Polars DataFrame.
    Much faster than pandas for large files.
    """
    if DBF is None:
        raise ImportError("dbfread required. Install with: uv add dbfread")

    logger.info(f"Reading DBF file: {dbf_path}")
    dbf = DBF(str(dbf_path), encoding='latin-1')

    # Build column-oriented data (more efficient for Polars)
    columns = {our_field: [] for our_field in COLUMN_MAPPING.values()}

    for i, record in enumerate(dbf):
        for dbf_field, our_field in COLUMN_MAPPING.items():
            value = record.get(dbf_field)
            # Coerce all values to strings for consistency
            columns[our_field].append(safe_str(value))

        if (i + 1) % 100000 == 0:
            logger.info(f"  Read {i + 1:,} records...")

    total_records = len(columns["folio"])
    logger.info(f"Total records read: {total_records:,}")

    # Create DataFrame from column dict (all strings initially)
    df = pl.DataFrame(columns)

    # Filter out records without folio
    df = df.filter(pl.col("folio").is_not_null() & (pl.col("folio") != ""))

    # Cast numeric columns
    numeric_cols = [
        "year_built", "beds", "baths", "stories", "units", "buildings",
        "heated_area", "lot_size", "assessed_value", "market_value",
        "just_value", "land_value", "building_value", "extra_features_value",
        "taxable_value", "last_sale_price"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))

    # Cast date column
    if "last_sale_date" in df.columns:
        df = df.with_columns(pl.col("last_sale_date").str.to_date(strict=False))

    logger.info(f"DataFrame created with {len(df):,} rows, {len(df.columns)} columns")
    return df


def save_to_parquet(df: pl.DataFrame, name: str = "bulk_parcels") -> Path:
    """
    Save Polars DataFrame to Parquet file.

    Returns the path to the saved file.
    """
    ensure_directories()
    timestamp = now_utc().strftime("%Y%m%d")
    parquet_path = PARQUET_DIR / f"{name}_{timestamp}.parquet"

    logger.info(f"Saving to Parquet: {parquet_path}")
    df.write_parquet(parquet_path, compression="zstd")

    # Also save a "latest" symlink/copy for easy access
    latest_path = PARQUET_DIR / f"{name}_latest.parquet"
    df.write_parquet(latest_path, compression="zstd")

    logger.info(f"Parquet saved: {parquet_path.stat().st_size / 1024 / 1024:.1f} MB")
    return parquet_path


def load_from_parquet(name: str = "bulk_parcels") -> pl.DataFrame | None:
    """Load the latest Parquet file."""
    latest_path = PARQUET_DIR / f"{name}_latest.parquet"
    if latest_path.exists():
        return pl.read_parquet(latest_path)
    return None


def dbf_zip_to_polars(zip_path: Path, pattern: str = "parcel") -> pl.DataFrame:
    """
    Extract DBF from zip and convert to Polars.
    """
    logger.info(f"Extracting {pattern} from ZIP: {zip_path}")

    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            # Find the .dbf file matching pattern
            dbf_files = [n for n in zf.namelist() if n.lower().endswith('.dbf') and pattern in n.lower()]

            if not dbf_files:
                # Fallback: any dbf
                dbf_files = [n for n in zf.namelist() if n.lower().endswith('.dbf')]

            if not dbf_files:
                raise FileNotFoundError(f"No DBF file found in {zip_path}")

            dbf_name = dbf_files[0]
            logger.info(f"Found DBF file: {dbf_name}")

            zf.extract(dbf_name, tmpdir)
            dbf_path = Path(tmpdir) / dbf_name
            return dbf_to_polars(dbf_path)


def load_latlon_data(zip_path: Path) -> pl.DataFrame:
    """
    Load Lat/Lon data from zip file.
    Has specific column mapping different from main parcel file.
    """
    logger.info(f"Extracting LatLon from ZIP: {zip_path}")

    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            dbf_files = [n for n in zf.namelist() if n.lower().endswith('.dbf')]
            if not dbf_files:
                raise FileNotFoundError(f"No DBF found in {zip_path}")

            dbf_name = dbf_files[0]
            zf.extract(dbf_name, tmpdir)
            dbf_path = Path(tmpdir) / dbf_name

            logger.info(f"Reading LatLon DBF: {dbf_path}")
            if DBF is None:
                 raise ImportError("dbfread required")

            dbf = DBF(str(dbf_path), encoding='latin-1')

            # Map for LatLon file
            # Expected cols: FOLIO, LAT, LONG (or similar)
            data = {"folio": [], "latitude": [], "longitude": []}

            for record in dbf:
                folio = safe_str(record.get("FOLIO"))
                lat = safe_float(record.get("lat") or record.get("LAT") or record.get("LATITUDE"))
                lon = safe_float(record.get("lon") or record.get("LONG") or record.get("LONGITUDE"))

                if folio and lat and lon:
                    data["folio"].append(folio)
                    data["latitude"].append(lat)
                    data["longitude"].append(lon)

            df = pl.DataFrame(data)

            # Cast types
            df = df.with_columns([
                pl.col("latitude").cast(pl.Float64),
                pl.col("longitude").cast(pl.Float64)
            ])

            logger.info(f"Loaded LatLon data: {len(df):,} records")
            return df


def download_and_ingest(force: bool = False) -> dict:
    """
    Orchestrate download and ingestion of bulk data to Parquet.
    """
    if not force and not should_refresh():
        logger.info("Bulk data is up to date. Skipping download.")
        return {}

    logger.info("Starting bulk data auto-download...")

    # Run async downloader synchronously
    downloads = asyncio.run(download_latest_bulk_data(BULK_DATA_DIR))

    if not downloads.get("parcel_zip"):
        raise RuntimeError("Failed to download parcel data ZIP")

    # 1. Load Main Parcel Data
    logger.info("Processing Parcel Data...")
    df_parcels = dbf_zip_to_polars(downloads["parcel_zip"], "parcel")

    # 2. Load LatLon Data (if available)
    if downloads.get("latlon_zip"):
        try:
            logger.info("Processing LatLon Data...")
            df_latlon = load_latlon_data(downloads["latlon_zip"])

            # Join on FOLIO
            logger.info("Merging Parcel and LatLon data...")
            df_parcels = df_parcels.join(df_latlon, on="folio", how="left")

            logger.info(f"Merged dimensions: {df_parcels.shape}")
        except Exception as e:
            logger.error(f"Failed to merge LatLon data: {e}")

    # 3. Save to Parquet
    parquet_path = save_to_parquet(df_parcels)
    stats = {
        "records": len(df_parcels),
        "parquet_file": str(parquet_path)
    }

    # Update metadata
    metadata = get_ingest_metadata()
    metadata["last_ingest_date"] = now_utc().isoformat()
    metadata["last_record_count"] = len(df_parcels)
    metadata["last_parquet_file"] = str(parquet_path)
    save_ingest_metadata(metadata)

    return stats


if __name__ == "__main__":
    import sys

    ensure_directories()

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python -m src.ingest.bulk_parcel_ingest <path_to_file.dbf|.zip>")
        print("  python -m src.ingest.bulk_parcel_ingest --check-refresh")
        print("  python -m src.ingest.bulk_parcel_ingest --download")
        sys.exit(1)

    arg = sys.argv[1]

    if arg == "--download":
        stats = download_and_ingest(force=True)
        print(f"Download & Ingest Complete: {stats}")
    elif arg == "--check-refresh":
        if should_refresh():
            print("Refresh needed")
            sys.exit(0)
        else:
            print("No refresh needed")
            sys.exit(1)
    else:
        path = Path(arg)
        if not path.exists():
            print(f"File not found: {path}")
            sys.exit(1)

        if path.suffix.lower() == ".zip":
            df = dbf_zip_to_polars(path, "parcel")
        elif path.suffix.lower() == ".dbf":
            df = dbf_to_polars(path)
        else:
            print(f"Unsupported file type: {path.suffix}")
            sys.exit(1)

        parquet_path = save_to_parquet(df)
        print(f"\nIngestion complete! Parquet saved to: {parquet_path}")
        print(f"  Records: {len(df):,}")
