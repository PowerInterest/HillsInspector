"""
Bulk parcel data ingestion from HCPA shapefile/DBF.

Downloads and processes the weekly parcel data dump from Hillsborough County
Property Appraiser. This provides the "source of truth" for ~480,000 parcels.

IMPORTANT: Uses Polars (NOT Pandas) and Parquet for efficient storage.

Usage:
    python -m src.ingest.bulk_parcel_ingest                    # Download & ingest
    python -m src.ingest.bulk_parcel_ingest --local <file>     # Ingest local file
    python -m src.ingest.bulk_parcel_ingest --enrich           # Enrich auctions
    python -m src.ingest.bulk_parcel_ingest --validate         # Validate data

Data Source:
    https://www.hcpafl.org -> DOWNLOADS -> GIS DATA
    File: parcel_MM_DD_YYYY.zip (contains parcel.dbf)
    Updated: Weekly (Sundays) - we only refresh weekly
"""

import duckdb
import polars as pl
import tempfile
import zipfile
import json
from datetime import datetime
from pathlib import Path
from loguru import logger
import asyncio
from src.ingest.bulk_downloader import download_latest_bulk_data

# For reading DBF files (shapefile attribute table)
try:
    from dbfread import DBF
except ImportError:
    DBF = None
    logger.warning("dbfread not installed. Run: uv add dbfread")


# Paths
DATA_DIR = Path("data")
BULK_DATA_DIR = DATA_DIR / "bulk_data"
PARQUET_DIR = DATA_DIR / "parquet"
METADATA_FILE = BULK_DATA_DIR / "ingest_metadata.json"
DB_PATH = DATA_DIR / "property_master.db"

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

    last_date = datetime.fromisoformat(last_ingest)
    days_since = (datetime.now() - last_date).days

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
    timestamp = datetime.now().strftime("%Y%m%d")
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


def ingest_to_duckdb(df: pl.DataFrame, db_path: str = str(DB_PATH)) -> dict:
    """
    Ingest Polars DataFrame into DuckDB using efficient bulk insert.

    DuckDB can directly read from Polars DataFrames or Parquet files.
    """
    logger.info(f"Ingesting {len(df):,} records to DuckDB: {db_path}")

    conn = duckdb.connect(db_path)

    # Create table if not exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bulk_parcels (
            folio VARCHAR PRIMARY KEY,
            pin VARCHAR,
            strap VARCHAR,
            owner_name VARCHAR,
            property_address VARCHAR,
            city VARCHAR,
            zip_code VARCHAR,
            land_use VARCHAR,
            land_use_desc VARCHAR,
            year_built INTEGER,
            beds FLOAT,
            baths FLOAT,
            stories FLOAT,
            units INTEGER,
            buildings INTEGER,
            heated_area FLOAT,
            lot_size FLOAT,
            assessed_value FLOAT,
            market_value FLOAT,
            just_value FLOAT,
            land_value FLOAT,
            building_value FLOAT,
            extra_features_value FLOAT,
            taxable_value FLOAT,
            last_sale_date DATE,
            last_sale_price FLOAT,
            raw_type VARCHAR,
            raw_sub VARCHAR,
            raw_taxdist VARCHAR,
            raw_muni VARCHAR,
            raw_legal1 VARCHAR,
            raw_legal2 VARCHAR,
            raw_legal3 VARCHAR,
            raw_legal4 VARCHAR,
            latitude FLOAT,
            longitude FLOAT,
            ingest_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create indices
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bulk_parcels_address ON bulk_parcels(property_address)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bulk_parcels_owner ON bulk_parcels(owner_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bulk_parcels_landuse ON bulk_parcels(land_use)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bulk_parcels_strap ON bulk_parcels(strap)")

    # Get count before
    try:
        before_count = conn.execute("SELECT COUNT(*) FROM bulk_parcels").fetchone()[0]
    except Exception:
        before_count = 0

    # Bulk insert using DuckDB's ability to read Polars DataFrames via Arrow
    # First clear existing data (faster than upsert for full refresh)
    conn.execute("DELETE FROM bulk_parcels")

    # Convert Polars to Arrow table for DuckDB ingestion
    arrow_table = df.to_arrow()
    conn.register("df_temp", arrow_table)

    conn.execute("""
        INSERT INTO bulk_parcels (
            folio, pin, strap, owner_name, property_address, city, zip_code,
            land_use, land_use_desc, year_built, beds, baths, stories, units, buildings,
            heated_area, lot_size, assessed_value, market_value, just_value,
            land_value, building_value, extra_features_value, taxable_value,
            last_sale_date, last_sale_price,
            raw_type, raw_sub, raw_taxdist, raw_muni, raw_legal1, raw_legal2, raw_legal3, raw_legal4,
            latitude, longitude
        )
        SELECT
            folio, pin, strap, owner_name, property_address, city, zip_code,
            land_use, land_use_desc, year_built, beds, baths, stories, units, buildings,
            heated_area, lot_size, assessed_value, market_value, just_value,
            land_value, building_value, extra_features_value, taxable_value,
            last_sale_date, last_sale_price,
            raw_type, raw_sub, raw_taxdist, raw_muni, raw_legal1, raw_legal2, raw_legal3, raw_legal4,
            latitude, longitude
        FROM df_temp
    """)

    # Get count after
    after_count = conn.execute("SELECT COUNT(*) FROM bulk_parcels").fetchone()[0]

    conn.close()

    stats = {
        "records_before": before_count,
        "records_after": after_count,
        "records_inserted": after_count,
    }
    logger.info(f"DuckDB ingestion complete: {stats}")
    return stats


def ingest_dbf_file(dbf_path: Path, db_path: str = str(DB_PATH), ingest_to_db: bool = True) -> dict:
    """
    Full ingestion pipeline: DBF -> Polars -> Parquet -> DuckDB (optional).
    """
    stats = {"source_file": str(dbf_path)}

    # Step 1: Read DBF into Polars
    df = dbf_to_polars(dbf_path)
    stats["records_read"] = len(df)

    # Step 2: Save to Parquet
    parquet_path = save_to_parquet(df)
    stats["parquet_file"] = str(parquet_path)

    # Step 3: Ingest to DuckDB (Optional)
    if ingest_to_db:
        db_stats = ingest_to_duckdb(df, db_path)
        stats.update(db_stats)
    else:
        logger.info("Skipping DuckDB ingestion (saved to Parquet only)")

    # Step 4: Update metadata
    metadata = get_ingest_metadata()
    metadata["last_ingest_date"] = datetime.now().isoformat()
    metadata["last_source_file"] = str(dbf_path)
    metadata["last_record_count"] = len(df)
    metadata["last_parquet_file"] = str(parquet_path)
    save_ingest_metadata(metadata)

    return stats


def ingest_from_zip(
    zip_path: Path,
    db_path: str = str(DB_PATH),
    ingest_to_db: bool = True,
) -> dict:
    """
    Extract and ingest parcel.dbf from a HCPA zip file.
    """
    logger.info(f"Extracting from ZIP: {zip_path}")

    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            # Find the .dbf file
            dbf_files = [n for n in zf.namelist() if n.lower().endswith('.dbf') and 'parcel' in n.lower()]

            if not dbf_files:
                dbf_files = [n for n in zf.namelist() if n.lower().endswith('.dbf')]

            if not dbf_files:
                raise FileNotFoundError(f"No DBF file found in {zip_path}")

            dbf_name = dbf_files[0]
            logger.info(f"Found DBF file: {dbf_name}")

            zf.extract(dbf_name, tmpdir)
            dbf_path = Path(tmpdir) / dbf_name
            return ingest_dbf_file(dbf_path, db_path, ingest_to_db=ingest_to_db)


def download_and_ingest(db_path: str = str(DB_PATH), force: bool = False, ingest_to_db: bool = False) -> dict:
    """
    Orchestrate download and ingestion of bulk data.
    Defaults to Parquet-only (ingest_to_db=False) to avoid DB bloat until needed.
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
            # Left join to keep all parcels, adding lat/lon where matches found
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

    # 4. Ingest to DuckDB (Optional)
    if ingest_to_db:
        # Note: We need to handle the new lat/lon columns in ingest_to_duckdb if we want them in the DB
        # For now, ingest_to_duckdb follows hardcoded schema.
        # We can update the schema to include lat/lon or just ignore them for the bulk table.
        # Let's verify if ingest_to_duckdb supports extra columns (it uses SELECT FROM df, so requires schema match)
        # We should update ingest_to_duckdb to generic loading or add columns.
        db_stats = ingest_to_duckdb(df_parcels, db_path)
        stats.update(db_stats)
    else:
        logger.info("Skipping DuckDB ingestion (saved to Parquet only)")

    # Update metadata
    metadata = get_ingest_metadata()
    metadata["last_ingest_date"] = datetime.now().isoformat()
    metadata["last_record_count"] = len(df_parcels)
    metadata["last_parquet_file"] = str(parquet_path)
    save_ingest_metadata(metadata)
    
    return stats


def enrich_auctions_from_bulk(db_path: str = str(DB_PATH)) -> dict:
    """
    Enrich the parcels table using bulk_parcels data.

    This fills in missing property details for auction properties
    using the comprehensive HCPA bulk data, including legal descriptions.

    NOTE: The auctions table uses STRAP (parcel_id column) while bulk_parcels
    uses FOLIO as the primary key. We join on STRAP to match records.
    """
    conn = duckdb.connect(db_path)

    # Check if bulk_parcels table exists, if not try to load from parquet
    try:
        count = conn.execute("SELECT COUNT(*) FROM bulk_parcels").fetchone()[0]
        if count == 0:
            raise Exception("Empty table")
        logger.info(f"bulk_parcels table has {count:,} records")
    except Exception:
        # Try to load from parquet file
        # Look for specific dated file first, then latest
        parquet_candidates = [
            BULK_DATA_DIR / "bulk_parcels_20251204.parquet",  # Specific file from user
            PARQUET_DIR / "bulk_parcels_latest.parquet",
        ]

        parquet_file = None
        for candidate in parquet_candidates:
            if candidate.exists():
                parquet_file = candidate
                break

        if parquet_file is None:
            logger.warning("No bulk_parcels table or parquet file found. Skipping enrichment.")
            return {
                "error": "No bulk_parcels data available",
                "hint": "Run: uv run python -m src.ingest.bulk_parcel_ingest <parcel.dbf>",
            }

        logger.info(f"Loading bulk_parcels from parquet: {parquet_file}")
        df = pl.read_parquet(parquet_file)
        ingest_to_duckdb(df, db_path)
        logger.success(f"Loaded {len(df):,} records from parquet")

    # Ensure parcels table has legal description columns
    conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS legal_description VARCHAR")
    conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS judgment_legal_description VARCHAR")
    conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS raw_legal1 VARCHAR")
    conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS raw_legal2 VARCHAR")
    conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS raw_legal3 VARCHAR")
    conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS raw_legal4 VARCHAR")
    conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS strap VARCHAR")
    conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS bulk_folio VARCHAR")
    # Add coordinate columns if they don't exist (might already be there from schema)
    conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS latitude FLOAT")
    conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS longitude FLOAT")

    # Count auctions without parcel data
    # NOTE: auctions.folio is actually STRAP format, bulk_parcels.strap matches it
    before = conn.execute("""
        SELECT COUNT(*) FROM auctions a
        LEFT JOIN parcels p ON a.folio = p.folio
        WHERE p.folio IS NULL
    """).fetchone()[0]

    # Insert parcels for all auctions that don't have them yet
    # Join auctions.folio (which is STRAP) to bulk_parcels.strap
    conn.execute("""
        INSERT INTO parcels (
            folio, parcel_id, bulk_folio, owner_name, property_address, city, zip_code,
            land_use, year_built, beds, baths, heated_area, lot_size,
            assessed_value, market_value, last_sale_date, last_sale_price,
            strap, raw_legal1, raw_legal2, raw_legal3, raw_legal4,
            legal_description, latitude, longitude
        )
        SELECT
            a.folio, a.folio, b.folio, b.owner_name, b.property_address, b.city, b.zip_code,
            b.land_use, b.year_built, b.beds, b.baths, b.heated_area, b.lot_size,
            b.assessed_value, b.market_value, b.last_sale_date, b.last_sale_price,
            b.strap, b.raw_legal1, b.raw_legal2, b.raw_legal3, b.raw_legal4,
            CONCAT_WS(' ', b.raw_legal1, b.raw_legal2, b.raw_legal3, b.raw_legal4),
            b.latitude, b.longitude
        FROM auctions a
        INNER JOIN bulk_parcels b ON a.folio = b.strap
        LEFT JOIN parcels p ON a.folio = p.folio
        WHERE p.folio IS NULL
    """)

    # Update existing parcels missing data (including legal descriptions)
    # Join on STRAP (parcels.folio = bulk_parcels.strap)
    conn.execute("""
        UPDATE parcels SET
            bulk_folio = b.folio,
            owner_name = COALESCE(parcels.owner_name, b.owner_name),
            property_address = COALESCE(parcels.property_address, b.property_address),
            city = COALESCE(parcels.city, b.city),
            zip_code = COALESCE(parcels.zip_code, b.zip_code),
            year_built = COALESCE(parcels.year_built, b.year_built),
            beds = COALESCE(parcels.beds, b.beds),
            baths = COALESCE(parcels.baths, b.baths),
            heated_area = COALESCE(parcels.heated_area, b.heated_area),
            lot_size = COALESCE(parcels.lot_size, b.lot_size),
            assessed_value = COALESCE(parcels.assessed_value, b.assessed_value),
            market_value = COALESCE(parcels.market_value, b.market_value),
            strap = COALESCE(parcels.strap, b.strap),
            raw_legal1 = COALESCE(parcels.raw_legal1, b.raw_legal1),
            raw_legal2 = COALESCE(parcels.raw_legal2, b.raw_legal2),
            raw_legal3 = COALESCE(parcels.raw_legal3, b.raw_legal3),
            raw_legal4 = COALESCE(parcels.raw_legal4, b.raw_legal4),
            legal_description = COALESCE(
                parcels.legal_description,
                CONCAT_WS(' ', b.raw_legal1, b.raw_legal2, b.raw_legal3, b.raw_legal4)
            ),
            latitude = COALESCE(parcels.latitude, b.latitude),
            longitude = COALESCE(parcels.longitude, b.longitude),
            updated_at = CURRENT_TIMESTAMP
        FROM bulk_parcels b
        WHERE parcels.folio = b.strap
          AND (parcels.owner_name IS NULL OR parcels.year_built IS NULL
               OR parcels.beds IS NULL OR parcels.legal_description IS NULL)
    """)

    # Count after
    after = conn.execute("""
        SELECT COUNT(*) FROM auctions a
        LEFT JOIN parcels p ON a.folio = p.folio
        WHERE p.folio IS NULL
    """).fetchone()[0]

    # Count parcels with legal descriptions now
    with_legal = conn.execute("""
        SELECT COUNT(*) FROM parcels
        WHERE legal_description IS NOT NULL AND legal_description != ''
    """).fetchone()[0]

    enriched = before - after
    conn.close()

    logger.info(f"Enriched {enriched} auction parcels from bulk data")
    logger.info(f"Parcels with legal descriptions: {with_legal}")
    return {
        "auctions_without_parcels_before": before,
        "auctions_without_parcels_after": after,
        "parcels_enriched": enriched,
        "parcels_with_legal_description": with_legal,
    }


def get_parcel_by_folio(folio: str, db_path: str = str(DB_PATH)) -> dict | None:
    """Quick lookup of a parcel by folio number."""
    conn = duckdb.connect(db_path)

    result = conn.execute("""
        SELECT * FROM bulk_parcels WHERE folio = ?
    """, [folio]).fetchone()

    if result:
        columns = [desc[0] for desc in conn.description]
        conn.close()
        return dict(zip(columns, result, strict=True))

    conn.close()
    return None


def ingest_dor_codes(dbf_path: Path, db_path: str = str(DB_PATH)) -> dict:
    """
    Ingest DOR (Department of Revenue) land use codes lookup table.

    Maps land use codes like '0100' to descriptions like 'SINGLE FAMILY R'.
    """
    if DBF is None:
        raise ImportError("dbfread required. Install with: uv add dbfread")

    logger.info(f"Reading DOR codes from: {dbf_path}")
    dbf = DBF(str(dbf_path), encoding='latin-1')

    # Build data
    codes = []
    descriptions = []
    for record in dbf:
        codes.append(safe_str(record.get("DORCODE")))
        descriptions.append(safe_str(record.get("DORDESCR")))

    # Create Polars DataFrame
    df = pl.DataFrame({
        "dor_code": codes,
        "description": descriptions,
    }).filter(pl.col("dor_code").is_not_null())

    logger.info(f"Loaded {len(df)} DOR codes")

    # Save to Parquet
    parquet_path = PARQUET_DIR / "dor_codes_latest.parquet"
    df.write_parquet(parquet_path, compression="zstd")

    # Ingest to DuckDB
    conn = duckdb.connect(db_path)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS dor_codes (
            dor_code VARCHAR PRIMARY KEY,
            description VARCHAR,
            ingest_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("DELETE FROM dor_codes")

    arrow_table = df.to_arrow()
    conn.register("df_temp", arrow_table)

    conn.execute("""
        INSERT INTO dor_codes (dor_code, description)
        SELECT dor_code, description FROM df_temp
    """)

    count = conn.execute("SELECT COUNT(*) FROM dor_codes").fetchone()[0]
    conn.close()

    logger.info(f"Ingested {count} DOR codes to DuckDB")
    return {"dor_codes_count": count, "parquet_file": str(parquet_path)}


def dbf_zip_to_polars(zip_path: Path, filename_pattern: str) -> pl.DataFrame:
    """Helper to extract a specific DBF from a zip and load it into Polars."""
    logger.info(f"Extracting {filename_pattern} from {zip_path}")
    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            files = [n for n in zf.namelist() if n.lower().endswith('.dbf') and filename_pattern.lower() in n.lower()]
            if not files:
                # Fallback: any DBF
                files = [n for n in zf.namelist() if n.lower().endswith('.dbf')]
            
            if not files:
                 raise FileNotFoundError(f"No matching DBF found in {zip_path}")
            
            target_file = files[0]
            zf.extract(target_file, tmpdir)
            return dbf_to_polars(Path(tmpdir) / target_file)


def load_latlon_data(zip_path: Path) -> pl.DataFrame:
    """
    Load LatLon data from zip.
    Expected columns: FOLIO, Name, lat, lon
    """
    if DBF is None:
         raise ImportError("dbfread required")

    logger.info(f"Loading LatLon data from {zip_path}")
    # We can reuse dbf_to_polars logic but we need to map columns differently
    # Since dbf_to_polars is hardcoded for parcel schema, we'll implement a simple reader here or genericize dbf_to_polars
    # For now, let's use a specialized reader since the schema is simple
    
    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            target = [n for n in zf.namelist() if n.lower().endswith('.dbf')][0]
            zf.extract(target, tmpdir)
            dbf_path = Path(tmpdir) / target
            
            dbf = DBF(str(dbf_path), encoding='latin-1')
            
            folios = []
            lats = []
            lons = []
            
            for record in dbf:
                folios.append(safe_str(record.get('FOLIO')))
                lats.append(safe_float(record.get('lat')))
                lons.append(safe_float(record.get('lon')))
                
            return pl.DataFrame({
                "folio": folios,
                "latitude": lats,
                "longitude": lons
            }).filter(pl.col("folio").is_not_null())


def ingest_subdivisions(dbf_path: Path, db_path: str = str(DB_PATH)) -> dict:
    """
    Ingest subdivision names lookup table.

    Maps subdivision codes to names and plat book references.
    """
    if DBF is None:
        raise ImportError("dbfread required. Install with: uv add dbfread")

    logger.info(f"Reading subdivisions from: {dbf_path}")
    dbf = DBF(str(dbf_path), encoding='latin-1')

    # Build data
    data = {
        "sub_code": [],
        "sub_name": [],
        "plat_book": [],
        "plat_page": [],
    }

    for record in dbf:
        data["sub_code"].append(safe_str(record.get("SUBCODE")))
        data["sub_name"].append(safe_str(record.get("SUBNAME")))
        data["plat_book"].append(safe_str(record.get("PLAT_BK")))
        data["plat_page"].append(safe_str(record.get("PAGE")))

    # Create Polars DataFrame and deduplicate by sub_code (keep first occurrence)
    df = pl.DataFrame(data).filter(
        pl.col("sub_code").is_not_null() & (pl.col("sub_code") != "")
    ).unique(subset=["sub_code"], keep="first")

    logger.info(f"Loaded {len(df)} unique subdivisions")

    # Save to Parquet
    parquet_path = PARQUET_DIR / "subdivisions_latest.parquet"
    df.write_parquet(parquet_path, compression="zstd")

    # Ingest to DuckDB
    conn = duckdb.connect(db_path)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS subdivisions (
            sub_code VARCHAR PRIMARY KEY,
            sub_name VARCHAR,
            plat_book VARCHAR,
            plat_page VARCHAR,
            ingest_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("DELETE FROM subdivisions")

    arrow_table = df.to_arrow()
    conn.register("df_temp", arrow_table)

    conn.execute("""
        INSERT INTO subdivisions (sub_code, sub_name, plat_book, plat_page)
        SELECT sub_code, sub_name, plat_book, plat_page FROM df_temp
    """)

    count = conn.execute("SELECT COUNT(*) FROM subdivisions").fetchone()[0]
    conn.close()

    logger.info(f"Ingested {count} subdivisions to DuckDB")
    return {"subdivisions_count": count, "parquet_file": str(parquet_path)}


def ingest_all_lookup_tables(bulk_data_dir: Path = BULK_DATA_DIR, db_path: str = str(DB_PATH)) -> dict:
    """
    Ingest all lookup tables from the bulk data directory.

    Looks for:
    - parcel_dor_names.dbf (DOR codes)
    - parcel_sub_names.dbf (subdivisions)
    """
    stats = {}

    dor_path = bulk_data_dir / "parcel_dor_names.dbf"
    if dor_path.exists():
        stats["dor_codes"] = ingest_dor_codes(dor_path, db_path)
    else:
        logger.warning(f"DOR codes file not found: {dor_path}")

    sub_path = bulk_data_dir / "parcel_sub_names.dbf"
    if sub_path.exists():
        stats["subdivisions"] = ingest_subdivisions(sub_path, db_path)
    else:
        logger.warning(f"Subdivisions file not found: {sub_path}")

    return stats


def validate_bulk_data(db_path: str = str(DB_PATH)) -> dict:
    """Run validation checks on the bulk_parcels table."""
    conn = duckdb.connect(db_path)

    stats = {}

    stats["total_records"] = conn.execute(
        "SELECT COUNT(*) FROM bulk_parcels"
    ).fetchone()[0]

    stats["has_address"] = conn.execute(
        "SELECT COUNT(*) FROM bulk_parcels WHERE property_address IS NOT NULL AND property_address != ''"
    ).fetchone()[0]

    stats["has_year_built"] = conn.execute(
        "SELECT COUNT(*) FROM bulk_parcels WHERE year_built IS NOT NULL AND year_built > 0"
    ).fetchone()[0]

    stats["has_beds_baths"] = conn.execute(
        "SELECT COUNT(*) FROM bulk_parcels WHERE beds IS NOT NULL AND baths IS NOT NULL"
    ).fetchone()[0]

    stats["has_market_value"] = conn.execute(
        "SELECT COUNT(*) FROM bulk_parcels WHERE market_value IS NOT NULL AND market_value > 0"
    ).fetchone()[0]

    stats["land_use_distribution"] = conn.execute("""
        SELECT land_use, COUNT(*) as cnt
        FROM bulk_parcels
        GROUP BY land_use
        ORDER BY cnt DESC
        LIMIT 10
    """).fetchall()

    stats["city_distribution"] = conn.execute("""
        SELECT city, COUNT(*) as cnt
        FROM bulk_parcels
        WHERE city IS NOT NULL AND city != ''
        GROUP BY city
        ORDER BY cnt DESC
    """).fetchall()

    conn.close()
    return stats


if __name__ == "__main__":
    import sys

    ensure_directories()

    if len(sys.argv) < 2:
        # Default: check if refresh needed and process local file
        print("Usage:")
        print("  python -m src.ingest.bulk_parcel_ingest <path_to_file.dbf|.zip>")
        print("  python -m src.ingest.bulk_parcel_ingest --enrich")
        print("  python -m src.ingest.bulk_parcel_ingest --validate")
        print("  python -m src.ingest.bulk_parcel_ingest --check-refresh")
        print("  python -m src.ingest.bulk_parcel_ingest --check-refresh")
        print("  python -m src.ingest.bulk_parcel_ingest --lookup-tables")
        print("  python -m src.ingest.bulk_parcel_ingest --download")
        sys.exit(1)

    arg = sys.argv[1]

    if arg == "--download":
        stats = download_and_ingest(force=True)
        print(f"Download & Ingest Complete: {stats}")
    elif arg == "--enrich":
        stats = enrich_auctions_from_bulk()
        print(f"Enrichment stats: {stats}")
    elif arg == "--validate":
        stats = validate_bulk_data()
        print("Bulk data validation:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
    elif arg == "--check-refresh":
        if should_refresh():
            print("Refresh needed")
            sys.exit(0)
        else:
            print("No refresh needed")
            sys.exit(1)
    elif arg == "--lookup-tables":
        stats = ingest_all_lookup_tables()
        print("Lookup tables ingestion:")
        for table_name, table_stats in stats.items():
            print(f"  {table_name}:")
            for key, value in table_stats.items():
                print(f"    {key}: {value}")
    else:
        path = Path(arg)
        if not path.exists():
            print(f"File not found: {path}")
            sys.exit(1)

        if path.suffix.lower() == ".zip":
            stats = ingest_from_zip(path)
        elif path.suffix.lower() == ".dbf":
            stats = ingest_dbf_file(path)
        else:
            print(f"Unsupported file type: {path.suffix}")
            sys.exit(1)

        print("\nIngestion complete!")
        for key, value in stats.items():
            print(f"  {key}: {value}")
