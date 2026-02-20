"""
Scraper Storage Service

Manages filesystem storage for all scraper outputs.
Organized by PROPERTY - all data for a property lives together.

Filesystem Structure:
    data/Foreclosure/
    ├── {case_number}/
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
    │       └── ...

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
"""

import json
import hashlib
from pathlib import Path
from typing import Optional, Dict, Any, TYPE_CHECKING
from loguru import logger
from src.utils.time import now_utc
from src.db.sqlite_paths import resolve_sqlite_db_path_str

if TYPE_CHECKING:
    from src.db.operations import PropertyDB


class ScraperStorage:
    """
    Manages filesystem storage for all scraper outputs, organized by property.
    Database tracking is handled by the orchestrator via DatabaseWriter (inbox pattern).
    """

    BASE_DIR = Path("data/Foreclosure")
    DB_PATH = resolve_sqlite_db_path_str()

    def __init__(
        self,
        db_path: str | None = None,
        db: "PropertyDB | None" = None,
    ):
        self.db = db
        if db_path:
            self.db_path = db_path
        elif db:
            self.db_path = db.db_path
        else:
            self.db_path = self.DB_PATH
        self.BASE_DIR.mkdir(parents=True, exist_ok=True)

    def _get_property_dir(self, property_id: str) -> Path:
        """Get or create directory for a property key (Case Number)."""
        safe_id = self._sanitize_filename(property_id)
        prop_dir = self.BASE_DIR / safe_id

        # Create subdirectories
        (prop_dir / "screenshots").mkdir(parents=True, exist_ok=True)
        (prop_dir / "vision").mkdir(parents=True, exist_ok=True)
        (prop_dir / "raw").mkdir(parents=True, exist_ok=True)
        (prop_dir / "documents").mkdir(parents=True, exist_ok=True)
        (prop_dir / "photos").mkdir(parents=True, exist_ok=True)

        return prop_dir

    @staticmethod
    def _sanitize_filename(name: str) -> str:
        """Sanitize a string for use as filename."""
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
        """Save a screenshot to the property's folder."""
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
        """Save VisionService output JSON."""
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
        """Save raw API response or scraped data."""
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
        """Check if a document already exists on disk."""
        safe_id = self._sanitize_filename(property_id)
        prop_dir = self.BASE_DIR / safe_id
        docs_dir = prop_dir / "documents"

        if not docs_dir.exists():
            return None

        extension = extension.lstrip(".").lower()

        if doc_id:
            filename = f"{doc_type}_{doc_id}.{extension}"
            filepath = docs_dir / filename
            if filepath.exists():
                return filepath
        else:
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
        """Save a document (PDF, image, etc.)."""
        prop_dir = self._get_property_dir(property_id)

        extension = extension.lstrip(".").lower()

        filename = doc_type
        if doc_id:
            filename += f"_{doc_id}"

        if not doc_id:
            filename += f"_{self._timestamp()}"

        filename += f".{extension}"

        filepath = prop_dir / "documents" / filename
        filepath.write_bytes(file_data)

        return f"documents/{filename}"

    # -------------------------------------------------------------------------
    # Cache Stubs (No-ops for Inbox Pattern)
    # -------------------------------------------------------------------------

    def needs_refresh(
        self, property_id: str, scraper: str, max_age_days: int = 30
    ) -> bool:
        """Always True (no DB cache tracking)."""
        return True

    def get_latest(self, property_id: str, scraper: str) -> None:
        """Get latest scrape record. Returns None (no DB cache tracking)."""
        return

    # -------------------------------------------------------------------------
    # Record Methods (No-ops for Inbox Pattern)
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
        """Record a scraper run (no-op: inbox pattern avoids DB locks in scrapers)."""
        status = "SUCCESS" if success else "FAILED"
        logger.debug(f"[No-DB] Recorded scrape for {property_id} ({scraper}): {status}")
        return 0

    async def record_scrape_async(
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
        """Async wrapper for record_scrape (no-op)."""
        return 0

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
