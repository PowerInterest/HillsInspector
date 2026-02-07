
import shutil
from pathlib import Path
from datetime import datetime
import polars as pl
from loguru import logger

from src.models.property import Property
from src.db.operations import PropertyDB
from src.utils.time import parse_date

class InboxScanner:
    def __init__(self, db: PropertyDB | None = None):
        self.db = db or PropertyDB()
        self.base_dir = Path("data/Foreclosure")

    def scan_and_ingest(self):
        """
        Scan all case directories for 'auction.parquet' files.
        Ingest them into the database and move to 'consumed/' folder.
        """
        logger.info("Starting Inbox Scan...")
        
        # Glob for all auction.parquet files in immediate subdirectories of data/Foreclosure
        # Pattern: data/Foreclosure/{case_number}/auction.parquet
        parquet_files = list(self.base_dir.glob("*/auction.parquet"))
        
        if not parquet_files:
            logger.info("Inbox empty: No auction.parquet files found.")
            return

        logger.info(f"Found {len(parquet_files)} inbox files to process.")
        
        count = 0
        for p_file in parquet_files:
            try:
                self._process_file(p_file)
                count += 1
            except Exception as e:
                logger.error(f"Failed to ingest {p_file}: {e}")

        logger.success(f"Inbox Scan Complete. Ingested {count}/{len(parquet_files)} files.")

    def _process_file(self, file_path: Path):
        case_dir = file_path.parent
        consumed_dir = case_dir / "consumed"
        consumed_dir.mkdir(exist_ok=True)
        
        # Read Parquet
        try:
            df = pl.read_parquet(file_path)
        except Exception as e:
            logger.error(f"Corrupt parquet file {file_path}: {e}")
            # Move to error folder? Or rename?
            error_path = file_path.with_suffix(".parquet.error")
            file_path.rename(error_path)
            return

        row_count = df.height
        if df.is_empty():
            logger.warning(f"Empty parquet file: {file_path}")
            # Move to consumed anyway to clear queue
            shutil.move(str(file_path), str(consumed_dir / file_path.name))
            return

        successes = 0
        failures = 0

        # Iterate rows and upsert
        for row in df.iter_rows(named=True):
            prop = self._row_to_property(row)
            if not prop:
                failures += 1
                continue
            try:
                self.db.upsert_auction(prop)
                # Also ensure status is tracked
                self.db.upsert_status(
                    case_number=prop.case_number,
                    parcel_id=prop.parcel_id,
                    auction_date=prop.auction_date,
                    auction_type="FORECLOSURE"
                )
                self.db.mark_status_step_complete(prop.case_number, "step_auction_scraped", 1)
                successes += 1
            except Exception as exc:  # noqa: BLE001
                failures += 1
                logger.error(f"Failed to upsert {prop.case_number}: {exc}")

        # Move to consumed
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_name = f"auction_{timestamp}.parquet"
        shutil.move(str(file_path), str(consumed_dir / new_name))
        logger.debug(f"Ingested and archived: {file_path} -> {consumed_dir / new_name}")
        logger.info(
            f"Ingested {successes}/{row_count} rows from {file_path.name} "
            f"(failures: {failures})"
        )

    def _row_to_property(self, row: dict) -> Property:
        # Convert row dict to Property object
        # Handle date parsing
        auction_date = None
        if row.get("auction_date"):
            # Parquet might store as date, datetime, or string
            raw_date = row["auction_date"]
            if isinstance(raw_date, str):
                auction_date = parse_date(raw_date)
            else:
                auction_date = raw_date 

        return Property(
            case_number=row["case_number"],
            parcel_id=row.get("parcel_id"),
            address=row.get("address"),
            city=row.get("city"),
            zip_code=row.get("zip_code"),
            assessed_value=row.get("assessed_value"),
            final_judgment_amount=row.get("final_judgment_amount"),
            auction_date=auction_date,
            auction_type=row.get("auction_type", "FORECLOSURE"),
            plaintiff=row.get("plaintiff"),
            defendant=row.get("defendant"),
            instrument_number=row.get("instrument_number"),
            legal_description=row.get("legal_description")
        )

if __name__ == "__main__":
    scanner = InboxScanner()
    scanner.scan_and_ingest()
