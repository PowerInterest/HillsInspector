"""
Scraper Storage Service

Manages filesystem storage and database tracking for all scraper outputs.
Organized by PROPERTY - all data for a property lives together.

Filesystem Structure:
    data/properties/
    ├── {property_id}/
    │   ├── screenshots/
    │   │   ├── {scraper}_{timestamp}.png
    │   │   └── ...
    │   ├── vision/
    │   │   ├── {scraper}_{timestamp}.json
    │   │   └── ...
    │   ├── raw/
    │   │   ├── {scraper}_{timestamp}.json
    │   │   └── ...
    │   └── documents/
    │       ├── final_judgment.pdf
    │       ├── deed_12345.pdf
    │       ├── photo_evidence.jpg
    │       └── ...

Database Table (scraper_outputs):
    - Tracks all scraper runs per property
    - Links to filesystem paths
    - Stores extraction status
    - Enables re-processing without re-scraping

Usage:
    storage = ScraperStorage()

    # Save screenshot
    screenshot_path = storage.save_screenshot(
        property_id="1234567890",
        scraper="permits",
        image_data=screenshot_bytes
    )

    # Save vision output
    vision_path = storage.save_vision_output(
        property_id="1234567890",
        scraper="permits",
        vision_data={"permits": [...]},
        screenshot_path=screenshot_path
    )

    # Check if we need to re-scrape
    if storage.needs_refresh(property_id="1234567890", scraper="permits"):
        # Re-scrape
    else:
        # Use cached data
        cached = storage.get_latest(property_id="1234567890", scraper="permits")
"""

import json
import hashlib
import os
from contextlib import suppress
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List, TYPE_CHECKING
from dataclasses import dataclass
import duckdb
from loguru import logger
from src.utils.time import ensure_duckdb_utc, now_utc, now_utc_naive

if TYPE_CHECKING:
    from src.db.operations import PropertyDB


@dataclass
class ScraperRecord:
    """Represents a single scraper output record."""
    id: Optional[int] = None
    property_id: str = ""
    scraper: str = ""  # fema, permits, sunbiz, realtor, zillow, hcpa, ori, etc.

    # Timestamps
    scraped_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None

    # File paths (relative to data/properties/{property_id}/)
    screenshot_path: Optional[str] = None
    vision_output_path: Optional[str] = None
    raw_data_path: Optional[str] = None
    source_url: Optional[str] = None

    # Processing status
    prompt_version: Optional[str] = None
    extraction_success: bool = False
    error_message: Optional[str] = None

    # Quick access to key extracted values (JSON string)
    extracted_summary: Optional[str] = None


class ScraperStorage:
    """
    Manages storage for all scraper outputs, organized by property.
    """

    BASE_DIR = Path("data/properties")
    DB_PATH = "data/property_master.db"

    def __init__(
        self,
        db_path: str | None = None,
        skip_db_init: bool = False,
        db: "PropertyDB | None" = None,
    ):
        """
        Initialize storage service.

        Args:
            db_path: Optional custom database path
            skip_db_init: Skip database initialization (for read-only or no-DB mode)
        """
        self.db = db
        if db_path:
            self.db_path = db_path
        elif db:
            self.db_path = db.db_path
        else:
            self.db_path = self.DB_PATH
        self.BASE_DIR.mkdir(parents=True, exist_ok=True)
        self._skip_db = skip_db_init or os.environ.get("HILLS_SCRAPER_STORAGE_SKIP_INIT") == "1"
        if not self._skip_db:
            self._init_database()

    def _init_database(self):
        """Create the scraper_outputs table if it doesn't exist."""
        conn, should_close = self._get_conn()

        # Check if table exists and needs migration
        with suppress(Exception):
            result = conn.execute("""
                SELECT column_name, column_default FROM information_schema.columns
                WHERE table_name = 'scraper_outputs' AND column_name = 'id'
            """).fetchone()

            # If table exists but id doesn't have auto-increment, drop and recreate
            if result and (result[1] is None or 'nextval' not in str(result[1])):
                logger.info("Migrating scraper_outputs table to use auto-increment IDs")
                conn.execute("DROP TABLE IF EXISTS scraper_outputs")
                conn.execute("DROP SEQUENCE IF EXISTS scraper_outputs_id_seq")

        # Check if source_url column exists
        with suppress(Exception):
            result = conn.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'scraper_outputs' AND column_name = 'source_url'
            """).fetchone()
            
            if not result:
                logger.info("Adding source_url column to scraper_outputs")
                conn.execute("ALTER TABLE scraper_outputs ADD COLUMN source_url VARCHAR")

        # Create sequence for auto-increment IDs
        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS scraper_outputs_id_seq START 1
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS scraper_outputs (
                id INTEGER PRIMARY KEY DEFAULT nextval('scraper_outputs_id_seq'),
                property_id VARCHAR NOT NULL,
                scraper VARCHAR NOT NULL,

                scraped_at TIMESTAMP,
                processed_at TIMESTAMP,

                screenshot_path VARCHAR,
                vision_output_path VARCHAR,


                raw_data_path VARCHAR,
                source_url VARCHAR,

                prompt_version VARCHAR,
                extraction_success BOOLEAN DEFAULT FALSE,
                error_message VARCHAR,

                extracted_summary VARCHAR,

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create indexes
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_scraper_outputs_property
            ON scraper_outputs(property_id)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_scraper_outputs_lookup
            ON scraper_outputs(property_id, scraper)
        """)
        if should_close:
            conn.close()

    def _get_conn(self):
        if self.db:
            return self.db.connect(), False
        conn = duckdb.connect(self.db_path)
        ensure_duckdb_utc(conn)
        return conn, True

    def _get_property_dir(self, property_id: str) -> Path:
        """Get or create directory for a property."""
        safe_id = self._sanitize_filename(property_id)
        prop_dir = self.BASE_DIR / safe_id

        # Create subdirectories
        (prop_dir / "screenshots").mkdir(parents=True, exist_ok=True)
        (prop_dir / "vision").mkdir(parents=True, exist_ok=True)
        (prop_dir / "raw").mkdir(parents=True, exist_ok=True)
        (prop_dir / "documents").mkdir(parents=True, exist_ok=True)

        return prop_dir

    @staticmethod
    def _sanitize_filename(name: str) -> str:
        """
        Sanitize a string for use as filename.
        Standardizes Folio numbers by removing dashes.
        """
        # Remove dashes from Folio numbers (usually 17-20 chars with dashes)
        # Example: 123456-7890 -> 1234567890
        safe = name.replace("-", "").replace(" ", "").replace(":", "_")
        safe = safe.replace("/", "_").replace("\\", "_").replace(",", "").replace("#", "")
        
        if len(safe) > 100:
            hash_suffix = hashlib.md5(name.encode()).hexdigest()[:8]
            safe = safe[:90] + "_" + hash_suffix
        return safe

    @staticmethod
    def _timestamp() -> str:
        """Generate timestamp string for filenames."""
        return now_utc().strftime("%Y%m%d_%H%M%S")

    # -------------------------------------------------------------------------
    # Save Methods
    # -------------------------------------------------------------------------

    def save_screenshot(
        self,
        property_id: str,
        scraper: str,
        image_data: bytes,
        context: str = ""
    ) -> str:
        """
        Save a screenshot to the property's folder.

        Args:
            property_id: Property identifier (folio)
            scraper: Scraper name (permits, fema, realtor, etc.)
            image_data: Raw image bytes
            context: Optional context suffix (e.g., "page2")

        Returns:
            Path relative to property folder (screenshots/{scraper}_{timestamp}.png)
        """
        prop_dir = self._get_property_dir(property_id)
        timestamp = self._timestamp()

        filename = f"{scraper}_{timestamp}"
        if context:
            filename += f"_{context}"
        filename += ".png"

        filepath = prop_dir / "screenshots" / filename
        filepath.write_bytes(image_data)

        relative_path = f"screenshots/{filename}"
        logger.debug(f"Saved screenshot: {property_id}/{relative_path}")
        return relative_path

    def save_screenshot_from_file(
        self,
        property_id: str,
        scraper: str,
        source_path: str,
        context: str = ""
    ) -> str:
        """Copy an existing screenshot file to property storage."""
        source = Path(source_path)
        if not source.exists():
            raise FileNotFoundError(f"Screenshot not found: {source_path}")

        return self.save_screenshot(
            property_id=property_id,
            scraper=scraper,
            image_data=source.read_bytes(),
            context=context
        )

    def save_vision_output(
        self,
        property_id: str,
        scraper: str,
        vision_data: Dict[str, Any],
        screenshot_path: Optional[str] = None,
        prompt_version: str = "v1",
        context: str = ""
    ) -> str:
        """
        Save VisionService output JSON.

        Args:
            property_id: Property identifier
            scraper: Scraper name
            vision_data: Extracted data from VisionService
            screenshot_path: Associated screenshot (relative path)
            prompt_version: Version of prompt used
            context: Optional context to append to filename (e.g., case number)

        Returns:
            Path relative to property folder
        """
        prop_dir = self._get_property_dir(property_id)
        timestamp = self._timestamp()

        suffix = ""
        if context:
            safe_context = (
                str(context)
                .replace(" ", "_")
                .replace("/", "_")
                .replace("\\", "_")
                .replace(":", "_")
            )
            suffix = f"_{safe_context}"

        filename = f"{scraper}_{timestamp}{suffix}_{prompt_version}.json"
        filepath = prop_dir / "vision" / filename

        output = {
            "scraper": scraper,
            "extracted_at": now_utc().isoformat(),
            "prompt_version": prompt_version,
            "context": context or None,
            "screenshot": screenshot_path,
            "data": vision_data
        }

        with open(filepath, "w") as f:
            json.dump(output, f, indent=2, default=str)

        relative_path = f"vision/{filename}"
        logger.debug(f"Saved vision output: {property_id}/{relative_path}")
        return relative_path

    def save_raw_data(
        self,
        property_id: str,
        scraper: str,
        data: Dict | str | bytes,
        context: str = "",
        extension: str = "json"
    ) -> str:
        """
        Save raw API response or scraped data.

        Args:
            property_id: Property identifier
            scraper: Scraper name
            data: Raw data
            context: Optional context
            extension: File extension (json, html, txt)

        Returns:
            Path relative to property folder
        """
        prop_dir = self._get_property_dir(property_id)
        timestamp = self._timestamp()

        filename = f"{scraper}_{timestamp}"
        if context:
            filename += f"_{context}"
        filename += f".{extension}"

        filepath = prop_dir / "raw" / filename

        if isinstance(data, dict):
            with open(filepath, "w") as f:
                json.dump(data, f, indent=2, default=str)
        elif isinstance(data, bytes):
            filepath.write_bytes(data)
        else:
            filepath.write_text(str(data))

        return f"raw/{filename}"

    def document_exists(
        self,
        property_id: str,
        doc_type: str,
        doc_id: str = "",
        extension: str = "pdf"
    ) -> Optional[Path]:
        """
        Check if a document already exists on disk.

        Args:
            property_id: Property identifier
            doc_type: Type (final_judgment, deed, mortgage, photo, etc.)
            doc_id: Optional document ID (for uniqueness)
            extension: File extension (pdf, jpg, docx, etc.)

        Returns:
            Path to existing file if found, None otherwise
        """
        safe_id = self._sanitize_filename(property_id)
        prop_dir = self.BASE_DIR / safe_id
        docs_dir = prop_dir / "documents"

        if not docs_dir.exists():
            return None

        extension = extension.lstrip(".").lower()

        if doc_id:
            # Exact match with doc_id
            filename = f"{doc_type}_{doc_id}.{extension}"
            filepath = docs_dir / filename
            if filepath.exists():
                return filepath
        else:
            # Glob for any file matching the doc_type pattern
            matches = list(docs_dir.glob(f"{doc_type}*.{extension}"))
            if matches:
                return matches[0]

        return None

    def save_document(
        self,
        property_id: str,
        file_data: bytes,
        doc_type: str,
        doc_id: str = "",
        extension: str = "pdf"
    ) -> str:
        """
        Save a document (PDF, image, etc.).

        Args:
            property_id: Property identifier
            file_data: File bytes
            doc_type: Type (final_judgment, deed, mortgage, photo, etc.)
            doc_id: Optional document ID (for uniqueness)
            extension: File extension (pdf, jpg, docx, etc.)

        Returns:
            Path relative to property folder
        """
        prop_dir = self._get_property_dir(property_id)

        # Sanitize inputs
        extension = extension.lstrip(".").lower()
        
        filename = doc_type
        if doc_id:
            filename += f"_{doc_id}"
        
        # Add timestamp if no ID to prevent overwrites
        if not doc_id:
            filename += f"_{self._timestamp()}"
            
        filename += f".{extension}"

        filepath = prop_dir / "documents" / filename
        filepath.write_bytes(file_data)

        return f"documents/{filename}"

    # -------------------------------------------------------------------------
    # Record Methods (Database)
    # -------------------------------------------------------------------------

    def record_scrape(
        self,
        property_id: str,
        scraper: str,
        screenshot_path: Optional[str] = None,
        vision_output_path: Optional[str] = None,
        raw_data_path: Optional[str] = None,
        vision_data: Optional[Dict] = None,
        prompt_version: Optional[str] = None,
        success: bool = True,
        error: Optional[str] = None,
        source_url: Optional[str] = None
    ) -> Optional[int]:
        """
        Record a scraper run in the database.

        Returns:
            Record ID (or 0 if DB skipped)
        """
        if self._skip_db:
            logger.debug(f"Skipping DB record for {scraper} on {property_id} (skip_db=True)")
            return 0

        conn, should_close = self._get_conn()

        summary = None
        if vision_data:
            summary = json.dumps(self._extract_summary(scraper, vision_data))

        conn.execute("""
            INSERT INTO scraper_outputs (
                property_id, scraper,
                scraped_at, processed_at,
                screenshot_path, vision_output_path, raw_data_path,
                prompt_version, extraction_success, error_message,
                extracted_summary, source_url, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, [
            property_id,
            scraper,
            now_utc_naive(),
            now_utc_naive() if vision_output_path else None,
            screenshot_path,
            vision_output_path,
            raw_data_path,
            prompt_version,
            success,
            error,
            summary,
            source_url
        ])

        # Also save to property_sources table for easy access
        if source_url:
            logger.info(f"Saving source URL for {property_id}: {source_url}")
            try:
                if self.db:
                    self.db.save_source(property_id, scraper, source_url)
                else:
                    from src.db.operations import PropertyDB
                    with PropertyDB(self.db_path) as db:
                        db.save_source(property_id, scraper, source_url)
                logger.info("Source saved successfully")
            except Exception as e:
                logger.warning(f"Failed to save source to property_sources: {e}")
        else:
            logger.info("No source URL to save")

        result = conn.execute("""
            SELECT MAX(id) FROM scraper_outputs
            WHERE property_id = ? AND scraper = ?
        """, [property_id, scraper]).fetchone()

        if should_close:
            conn.close()
        return result[0] if result else None

    def _extract_summary(self, scraper: str, vision_data: Dict) -> Dict:
        """Extract key summary fields from vision data."""
        if scraper == "fema":
            return {
                "flood_zone": vision_data.get("flood_zone"),
                "risk_level": vision_data.get("risk_level"),
                "insurance_required": vision_data.get("insurance_required")
            }
        if scraper == "permits":
            permits = vision_data.get("permits", [])
            return {
                "total": len(permits),
                "open": sum(1 for p in permits if p.get("status", "").upper() not in ["FINALED", "CLOSED"]),
            }
        if scraper == "sunbiz":
            entities = vision_data if isinstance(vision_data, list) else vision_data.get("entities", [])
            return {
                "found": len(entities),
                "active": sum(1 for e in entities if "ACTIVE" in str(e.get("status", "")).upper())
            }
        if scraper in ["realtor", "zillow"]:
            return {
                "price": vision_data.get("list_price") or vision_data.get("price"),
                "zestimate": vision_data.get("zestimate"),
                "hoa": vision_data.get("hoa_fee"),
                "status": vision_data.get("listing_status")
            }
        return {}

    # -------------------------------------------------------------------------
    # Query Methods
    # -------------------------------------------------------------------------

    def get_latest(
        self,
        property_id: str,
        scraper: str
    ) -> Optional[ScraperRecord]:
        """Get the most recent scraper output for a property/scraper combo."""
        conn, should_close = self._get_conn()

        result = conn.execute("""
            SELECT * FROM scraper_outputs
            WHERE property_id = ? AND scraper = ?
            ORDER BY scraped_at DESC LIMIT 1
        """, [property_id, scraper]).fetchone()

        if should_close:
            conn.close()

        if not result:
            return None

        return self._row_to_record(result)

    def get_all_for_property(self, property_id: str) -> List[ScraperRecord]:
        """Get all scraper outputs for a property."""
        conn, should_close = self._get_conn()

        results = conn.execute("""
            SELECT * FROM scraper_outputs
            WHERE property_id = ?
            ORDER BY scraper, scraped_at DESC
        """, [property_id]).fetchall()

        if should_close:
            conn.close()
        return [self._row_to_record(r) for r in results]

    def needs_refresh(
        self,
        property_id: str,
        scraper: str,
        max_age_days: int = 7
    ) -> bool:
        """Check if we need to re-scrape."""
        latest = self.get_latest(property_id, scraper)

        if not latest or not latest.scraped_at:
            return True

        scraped_at = latest.scraped_at
        if not scraped_at:
            return True
        now = now_utc_naive()
        if isinstance(scraped_at, datetime) and scraped_at.tzinfo:
            now = now_utc()
        age = now - scraped_at
        return age > timedelta(days=max_age_days)

    def get_unprocessed(self, scraper: str, limit: int = 100) -> List[ScraperRecord]:
        """Get screenshots that haven't been processed by VisionService."""
        conn, should_close = self._get_conn()

        results = conn.execute("""
            SELECT * FROM scraper_outputs
            WHERE scraper = ?
            AND screenshot_path IS NOT NULL
            AND (vision_output_path IS NULL OR extraction_success = FALSE)
            ORDER BY scraped_at DESC
            LIMIT ?
        """, [scraper, limit]).fetchall()

        if should_close:
            conn.close()
        return [self._row_to_record(r) for r in results]

    def _row_to_record(self, row) -> ScraperRecord:
        """Convert database row to ScraperRecord."""
        return ScraperRecord(
            id=row[0],
            property_id=row[1],
            scraper=row[2],
            scraped_at=row[3],
            processed_at=row[4],
            screenshot_path=row[5],
            vision_output_path=row[6],
            raw_data_path=row[7],
            prompt_version=row[8],
            extraction_success=row[9],
            error_message=row[10],
            extracted_summary=row[11],
            source_url=row[12] if len(row) > 12 else None
        )

    # -------------------------------------------------------------------------
    # Path Helpers
    # -------------------------------------------------------------------------

    def get_full_path(self, property_id: str, relative_path: str) -> Path:
        """Get absolute path from property-relative path."""
        safe_id = self._sanitize_filename(property_id)
        return self.BASE_DIR / safe_id / relative_path

    def load_vision_output(self, property_id: str, relative_path: str) -> Optional[Dict]:
        """Load vision output JSON."""
        filepath = self.get_full_path(property_id, relative_path)
        if not filepath.exists():
            return None
        with open(filepath) as f:
            return json.load(f)

    def load_raw_data(self, property_id: str, relative_path: str) -> Optional[Dict | str]:
        """Load raw data (JSON preferred, else text)."""
        filepath = self.get_full_path(property_id, relative_path)
        if not filepath.exists():
            return None
        if filepath.suffix.lower() == ".json":
            with open(filepath) as f:
                return json.load(f)
        return filepath.read_text()

    # -------------------------------------------------------------------------
    # Re-processing
    # -------------------------------------------------------------------------

    def update_vision_output(
        self,
        record_id: int,
        vision_output_path: str,
        prompt_version: str,
        success: bool,
        error: Optional[str] = None,
        summary: Optional[Dict] = None
    ):
        """Update an existing record with new vision processing results."""
        conn, should_close = self._get_conn()

        conn.execute("""
            UPDATE scraper_outputs SET
                vision_output_path = ?,
                prompt_version = ?,
                extraction_success = ?,
                error_message = ?,
                extracted_summary = ?,
                processed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, [
            vision_output_path,
            prompt_version,
            success,
            error,
            json.dumps(summary) if summary else None,
            record_id
        ])

        if should_close:
            conn.close()


def reprocess_screenshots(scraper: str, prompt_version: str = "v2", limit: int = 100):
    """
    Re-process screenshots with updated VisionService prompts.

    Usage:
        from src.services.scraper_storage import reprocess_screenshots
        reprocess_screenshots("permits", prompt_version="v2")
    """
    from src.services.vision_service import VisionService

    storage = ScraperStorage()
    vision = VisionService()

    unprocessed = storage.get_unprocessed(scraper, limit=limit)
    logger.info(f"Re-processing {len(unprocessed)} screenshots for {scraper}")

    for record in unprocessed:
        if not record.screenshot_path:
            continue
        if record.id is None:
            continue

        screenshot_path = storage.get_full_path(record.property_id, record.screenshot_path)

        if not screenshot_path.exists():
            logger.warning(f"Screenshot not found: {screenshot_path}")
            continue

        try:
            # Call appropriate vision method
            if scraper == "permits":
                vision_data = vision.extract_permit_results(str(screenshot_path))
            elif scraper == "realtor":
                vision_data = vision.extract_realtor_listing(str(screenshot_path))
            elif scraper == "zillow":
                vision_data = vision.extract_market_listing(str(screenshot_path))
            elif scraper == "hcpa":
                vision_data = vision.extract_hcpa_details(str(screenshot_path))
            else:
                logger.warning(f"No vision method for: {scraper}")
                continue

            # Save new output
            vision_path = storage.save_vision_output(
                property_id=record.property_id,
                scraper=scraper,
                vision_data=vision_data or {},
                screenshot_path=record.screenshot_path,
                prompt_version=prompt_version
            )

            storage.update_vision_output(
                record_id=record.id,
                vision_output_path=vision_path,
                prompt_version=prompt_version,
                success=vision_data is not None,
                error=None if vision_data else "Vision returned None",
                summary=storage._extract_summary(scraper, vision_data) if vision_data else None  # noqa: SLF001
            )

            logger.info(f"Re-processed {record.property_id}: success={vision_data is not None}")

        except Exception as e:
            logger.error(f"Error re-processing {record.property_id}: {e}")
            storage.update_vision_output(
                record_id=record.id,
                vision_output_path="",
                prompt_version=prompt_version,
                success=False,
                error=str(e)
            )


if __name__ == "__main__":
    # Test
    storage = ScraperStorage()

    test_id = "TEST_FOLIO_123"

    # Save screenshot
    fake_png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 100
    ss_path = storage.save_screenshot(test_id, "permits", fake_png)
    print(f"Screenshot: {ss_path}")

    # Save vision output
    vision_data = {"permits": [{"number": "BLD-001", "status": "Finaled"}]}
    vis_path = storage.save_vision_output(test_id, "permits", vision_data, ss_path)
    print(f"Vision: {vis_path}")

    # Record
    rec_id = storage.record_scrape(
        property_id=test_id,
        scraper="permits",
        screenshot_path=ss_path,
        vision_output_path=vis_path,
        vision_data=vision_data,
        prompt_version="v1",
        success=True
    )
    print(f"Record ID: {rec_id}")

    # Query
    latest = storage.get_latest(test_id, "permits")
    print(f"Latest: {latest}")

    print(f"\nNeeds refresh: {storage.needs_refresh(test_id, 'permits')}")
    print("Done!")
