import json
import random
import re
import shutil
import subprocess
import sys
import time
from datetime import date
from typing import Any, Dict, List, Optional

from math import isnan
from loguru import logger
from homeharvest import scrape_property
from src.db.operations import PropertyDB


def _is_blocking_error(error: Exception) -> bool:
    """Check if an error indicates we're being blocked by Realtor.com."""
    error_str = str(error).lower()
    error_type = type(error).__name__

    blocking_indicators = [
        "retryerror",
        "forbidden",
        "403",
        "blocked",
        "rate limit",
        "too many requests",
        "429",
    ]

    return any(indicator in error_str or indicator in error_type.lower()
               for indicator in blocking_indicators)


def _get_installed_version() -> str | None:
    """Get currently installed homeharvest version."""
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "show", "homeharvest"],
            check=False, capture_output=True, text=True, timeout=30
        )
        for line in result.stdout.split("\n"):
            if line.startswith("Version:"):
                return line.split(":")[1].strip()
    except Exception as exc:
        logger.debug(f"Failed to read installed homeharvest version: {exc}")
    return None


def _get_latest_version() -> str | None:
    """Get latest homeharvest version from PyPI."""
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "index", "versions", "homeharvest"],
            check=False, capture_output=True, text=True, timeout=30
        )
        # Output format: "homeharvest (0.8.11)"
        if "(" in result.stdout:
            return result.stdout.split("(")[1].split(")")[0].strip()
    except Exception as exc:
        logger.debug(f"Failed to read latest homeharvest version: {exc}")
    return None


def upgrade_homeharvest() -> bool:
    """
    Upgrade homeharvest to latest version.

    Returns:
        True if upgrade was performed, False otherwise.
    """
    installed = _get_installed_version()
    latest = _get_latest_version()

    if not latest:
        logger.warning("Could not determine latest homeharvest version")
        return False

    if installed == latest:
        logger.info(f"HomeHarvest already at latest version ({installed})")
        return False

    logger.warning(f"Upgrading HomeHarvest: {installed} -> {latest}")

    try:
        uv_path = shutil.which("uv")
        if not uv_path:
            logger.error("uv not found on PATH; cannot upgrade homeharvest")
            return False

        result = subprocess.run(
            [uv_path, "pip", "install", "--upgrade", "homeharvest"],
            check=False, capture_output=True, text=True, timeout=120
        )

        if result.returncode == 0:
            logger.success(f"HomeHarvest upgraded to {latest}")
            return True
        logger.error(f"HomeHarvest upgrade failed: {result.stderr}")
        return False

    except Exception as e:
        logger.error(f"HomeHarvest upgrade error: {e}")
        return False


def run_homeharvest_subprocess(limit: int = 100) -> bool:
    """
    Run HomeHarvest enrichment in a fresh subprocess.

    This is used after upgrading HomeHarvest to use the new version
    without requiring a full script restart.

    Args:
        limit: Maximum number of properties to process

    Returns:
        True if subprocess started successfully, False otherwise.
    """
    script = f'''
from src.services.homeharvest_service import HomeHarvestService
from loguru import logger
import sys

logger.remove()
logger.add(sys.stdout, level="INFO")

hh = HomeHarvestService(auto_upgrade=False)  # Don't try to upgrade again
props = hh.get_pending_properties(limit={limit})
print(f"Found {{len(props)}} properties needing HomeHarvest enrichment")

if props:
    hh.fetch_and_save(props)
    print("Done!")
else:
    print("No properties need enrichment.")
'''

    logger.info(f"Spawning HomeHarvest subprocess with limit={limit}...")

    try:
        # Run in background so we don't block
        subprocess.Popen(
            [sys.executable, "-c", script],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        logger.success("HomeHarvest subprocess started with upgraded version")
        return True
    except Exception as e:
        logger.error(f"Failed to spawn HomeHarvest subprocess: {e}")
        return False


class HomeHarvestService:
    # Delay between requests (seconds) - Realtor.com is aggressive with rate limiting
    MIN_DELAY = 15.0  # Minimum delay between requests
    MAX_DELAY = 30.0  # Maximum delay (randomized for human-like behavior)

    def __init__(
        self,
        proxy: str | None = None,
        auto_upgrade: bool = True,
        db: PropertyDB | None = None,
    ):
        """
        Initialize HomeHarvest service.

        Args:
            proxy: Optional proxy URL in format 'http://user:pass@host:port'
            auto_upgrade: If True, automatically upgrade homeharvest on first blocking error
        """
        self.db = db or PropertyDB()
        self.proxy = proxy
        self.auto_upgrade = auto_upgrade
        self._upgrade_attempted = False  # Track if we've already tried upgrading this session

    def get_pending_properties(
        self,
        limit: int = 100,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get properties that need HomeHarvest enrichment.
        Criteria: Has address, is not marked as needing HCPA review, and no recent HomeHarvest record.
        """
        conn = self.db.connect()
        
        query = """
            SELECT DISTINCT
                COALESCE(a.parcel_id, a.folio) AS folio, 
                COALESCE(a.property_address, p.property_address) AS property_address, 
                p.city, 
                p.zip_code,
                a.case_number
            FROM auctions a
            INNER JOIN parcels p ON COALESCE(a.parcel_id, a.folio) = p.folio
            LEFT JOIN home_harvest h ON COALESCE(a.parcel_id, a.folio) = h.folio
            WHERE COALESCE(a.property_address, p.property_address) IS NOT NULL 
              AND COALESCE(a.property_address, p.property_address) != ''
              AND (a.hcpa_scrape_failed IS NULL OR a.hcpa_scrape_failed = 0) -- Only good properties
              AND (h.folio IS NULL OR h.created_at < date('now', '-7 days')) -- No recent data
              AND a.needs_homeharvest_enrichment = 1 -- Flag to be enriched
        """
        params = []

        if start_date and end_date:
            query += " AND a.auction_date >= ? AND a.auction_date <= ?"
            params.extend([start_date, end_date])
        elif start_date:
            query += " AND a.auction_date >= ?"
            params.append(start_date)
        elif end_date:
            query += " AND a.auction_date <= ?"
            params.append(end_date)

        query += " LIMIT ?"
        params.append(limit)

        results = conn.execute(query, params).fetchall()
        
        props = []
        for r in results:
            raw_addr = r[1].strip()
            # Fix HCPA format "CITY, FL- ZIP" -> "CITY, FL ZIP"
            clean_addr = raw_addr.replace("FL- ", "FL ").replace("  ", " ")
            
            # Check if address already looks complete (ends with FL + Zip)
            if re.search(r'FL\s+\d{5}', clean_addr):
                location = clean_addr
            else:
                city = r[2].strip() if r[2] else ""
                zip_c = r[3].strip() if r[3] else ""
                state = "FL"
                
                # If address doesn't have city, append it
                parts = [clean_addr]
                if city and city.upper() not in clean_addr.upper():
                    parts.append(city)
                if "FL" not in clean_addr.upper():
                    parts.append(state)
                if zip_c and zip_c not in clean_addr:
                    parts.append(zip_c)
                    
                location = ", ".join(parts).replace("FL, ", "FL ") # Fix potential double comma
            
            props.append({
                "folio": r[0],
                "location": location,
                "case_number": r[4]
            })
            
        return props

    def fetch_and_save(self, properties: List[Dict[str, Any]]):
        """
        Fetch data from HomeHarvest and save to DB.
        """
        if not properties:
            return

        logger.info(f"Fetching HomeHarvest data for {len(properties)} properties...")
        
        try:
            # listing_type="sold" is best for comps/history
            # But we might want "for_sale" if it's active. 
            # HomeHarvest defaults to "for_sale" if not specified? 
            # Let's try to get everything by NOT filtering type if possible, 
            # or try "sold" which is most valuable for analysis.
            # Actually, scrape_property takes listing_type. 
            # Let's try 'sold' first as we want history.
            
            # NOTE: passing multiple locations to HomeHarvest isn't directly supported 
            # in a single string, it expects one location string per call usually, 
            # unless we pass a list. The library supports a list of locations?
            # Looking at docs/code: scrape_property(location=...)
            # We'll loop to be safe and handle errors per property.
            
            for i, prop in enumerate(properties):
                # First request triggers auto-upgrade check if blocked
                success = self._process_single_property(
                    prop["folio"], prop["location"], proxy=self.proxy, is_first=(i == 0)
                )

                # If first request was blocked and no upgrade available, stop processing
                if i == 0 and not success and self._upgrade_attempted:
                    logger.error("First request blocked and no upgrade available. Stopping.")
                    break

                # Add delay between requests to avoid rate limiting
                # Skip delay after the last one
                if i < len(properties) - 1:
                    delay = random.uniform(self.MIN_DELAY, self.MAX_DELAY)  # noqa: S311
                    logger.info(f"Waiting {delay:.0f}s before next request ({i+1}/{len(properties)} done)...")
                    time.sleep(delay)
            
            # Close DB connection to flush WAL
            self.db.close()

        except Exception as e:
            logger.error(f"HomeHarvest batch error: {e}")
            self.db.close()

    def process_single_property(self, folio: str, location: str) -> bool:
        """Public wrapper for single-property processing."""
        return self._process_single_property(folio, location)

    def _process_single_property(
        self, folio: str, location: str, proxy: str | None = None, is_first: bool = False
    ) -> bool:
        """
        Process a single property.

        Args:
            folio: Property folio ID
            location: Address string
            proxy: Optional proxy URL
            is_first: If True, this is the first request - trigger auto-upgrade on blocking

        Returns:
            True if successful, False if blocked/failed
        """
        try:
            logger.info(f"Scraping: {location}")
            # We search for "sold" to get history/metadata.
            # Use parallel=False for single property lookups - it's slower but more reliable
            # to avoid triggering rate limits from multiple concurrent requests.
            kwargs: dict = {
                "location": location,
                "listing_type": "sold",
                "past_days": 3650,  # 10 years
                "parallel": False,  # Sequential requests to reduce rate limiting
            }
            if proxy:
                kwargs["proxy"] = proxy
            df = scrape_property(**kwargs)

            if df is None or df.empty:
                logger.warning(f"No data found for {location}")
                return True  # Not a blocking error, just no data

            # Take the most recent relevant record (usually the first one)
            # HomeHarvest returns a pandas DataFrame - convert to dict at boundary.
            row = df.iloc[0].to_dict()
            data = self._build_record_data(folio, row)
            self.insert_record_data(data)
            logger.success(f"Saved data for {folio}")
            return True

        except Exception as e:
            # Check if this is a blocking error
            if _is_blocking_error(e):
                logger.warning(f"Blocking error detected: {type(e).__name__}")

                # On first request, try to auto-upgrade
                if is_first and self.auto_upgrade and not self._upgrade_attempted:
                    self._upgrade_attempted = True
                    logger.info("First request blocked - attempting to upgrade homeharvest...")

                    if upgrade_homeharvest():
                        logger.success("Upgrade successful! Spawning subprocess with new version...")
                        # Spawn a fresh subprocess that will use the upgraded version
                        run_homeharvest_subprocess(limit=100)
                        # Signal that we handled this by spawning a subprocess
                        raise SystemExit(
                            "HomeHarvest upgraded and subprocess spawned. "
                            "Exiting current process - the subprocess will continue."
                        ) from None
                    logger.warning("No upgrade available or upgrade failed")

                return False

            logger.error(f"Error processing {location}: {e}")
            return False

    def fetch_record_data(
        self,
        folio: str,
        location: str,
        proxy: str | None = None,
        is_first: bool = False,
    ) -> tuple[Dict[str, Any] | None, str]:
        """Fetch HomeHarvest data and return (record dict, status)."""
        try:
            logger.info(f"Scraping: {location}")
            kwargs: dict = {
                "location": location,
                "listing_type": "sold",
                "past_days": 3650,
                "parallel": False,
            }
            if proxy:
                kwargs["proxy"] = proxy
            df = scrape_property(**kwargs)

            if df is None or df.empty:
                logger.warning(f"No data found for {location}")
                return None, "no_data"

            row = df.iloc[0]
            return self._build_record_data(folio, row), "ok"
        except Exception as e:
            if _is_blocking_error(e):
                logger.warning(f"Blocking error detected: {type(e).__name__}")

                if is_first and self.auto_upgrade and not self._upgrade_attempted:
                    self._upgrade_attempted = True
                    logger.info("First request blocked - attempting to upgrade homeharvest...")

                    if upgrade_homeharvest():
                        logger.success("Upgrade successful! Spawning subprocess with new version...")
                        run_homeharvest_subprocess(limit=100)
                        raise SystemExit(
                            "HomeHarvest upgraded and subprocess spawned. "
                            "Exiting current process - the subprocess will continue."
                        ) from None
                    logger.warning("No upgrade available or upgrade failed")

                return None, "blocked"

            logger.error(f"Error processing {location}: {e}")
            return None, "error"

    def _build_record_data(self, folio: str, row: Dict[str, Any]) -> Dict[str, Any]:
        def _is_na(v: Any) -> bool:
            """Check if value is None or NaN."""
            if v is None:
                return True
            try:
                return isinstance(v, float) and isnan(v)
            except (TypeError, ValueError):
                return False

        # Helper to get value safely
        def val(col, dtype=str):
            if col not in row: return None
            v = row[col]
            if _is_na(v): return None
            try:
                if dtype == 'json': return json.dumps(v, default=str)
                if dtype == 'bool': return bool(v)
                if dtype == 'int': return int(v)
                if dtype == 'float': return float(v)
                return str(v)
            except Exception:
                return None

        # Helper for dates
        def date_val(col):
            v = val(col)
            if not v: return None
            try:
                from datetime import datetime
                if hasattr(v, 'isoformat'):
                    return v.isoformat()
                return datetime.fromisoformat(str(v)).isoformat()
            except Exception:
                return str(v)

        # Map fields
        return {
            'folio': folio,
            'property_url': val('property_url'),
            'property_id': val('property_id'),
            'listing_id': val('listing_id'),
            'mls': val('mls'),
            'mls_id': val('mls_id'),
            'mls_status': val('mls_status'),
            'status': val('status'),
            
            'street': val('street'),
            'unit': val('unit'),
            'city': val('city'),
            'state': val('state'),
            'zip_code': val('zip_code'),
            'formatted_address': val('formatted_address'),
            
            'style': val('style'),
            'beds': val('beds', 'float'),
            'full_baths': val('full_baths', 'float'),
            'half_baths': val('half_baths', 'float'),
            'sqft': val('sqft', 'float'),
            'year_built': val('year_built', 'int'),
            'stories': val('stories', 'float'),
            'garage': val('parking_garage', 'float'), # Mapping parking_garage to garage
            'lot_sqft': val('lot_sqft', 'float'),
            'text_description': val('text'),
            
            'days_on_mls': val('days_on_mls', 'int'),
            'list_price': val('list_price', 'float'),
            'list_date': date_val('list_date'),
            'sold_price': val('sold_price', 'float'),
            'last_sold_date': date_val('last_sold_date'),
            'price_per_sqft': val('price_per_sqft', 'float'),
            'hoa_fee': val('hoa_fee', 'float'),
            'estimated_value': val('estimated_value', 'float'),
            
            'latitude': val('latitude', 'float'),
            'longitude': val('longitude', 'float'),
            'neighborhoods': val('neighborhoods'),
            'county': val('county'),
            'fips_code': val('fips_code'),
            
            'nearby_schools': val('nearby_schools', 'json'),
            'photos': val('photos', 'json'),
            'primary_photo': val('primary_photo'),
            'alt_photos': val('alt_photos', 'json'),
        }

    def insert_record_data(self, data: Dict[str, Any]) -> None:
        conn = self.db.connect()

        # Insert SQL
        columns = list(data.keys())
        placeholders = ', '.join(['?'] * len(columns))
        col_str = ', '.join(columns)
        
        # Upsert logic (delete existing for this folio then insert, or insert on conflict ignore)
        # Since folio isn't unique in this table (history?), we might want to keep history.
        # But schema says id is PK. Let's just insert a new record for now.
        # To avoid dups for same scrape, check if we have a recent one?
        # The get_pending_properties handles the check.
        
        conn.execute(f"""
            INSERT INTO home_harvest ({col_str})
            VALUES ({placeholders})
        """, list(data.values()))

if __name__ == "__main__":
    service = HomeHarvestService()
    props = service.get_pending_properties(limit=5)
    service.fetch_and_save(props)
