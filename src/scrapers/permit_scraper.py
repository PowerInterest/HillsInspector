"""
Per-property permit lookup via ArcGIS REST API (county) and Accela CSV export (Tampa).

Delegates to CountyPermitService and TampaPermitService for the actual work.
Data is written directly to PG tables: county_permits / tampa_accela_records.

Usage:
    scraper = PermitScraper()
    result = scraper.fetch_all(folio="0472200300", address="3006 W Julia St, Tampa, FL")
"""

import argparse
from pathlib import Path

from loguru import logger
from sqlalchemy import text

from src.services.CountyPermit import CountyPermitService
from src.services.TampaPermit import TampaPermitService
from sunbiz.db import get_engine, resolve_pg_dsn


class PermitScraper:
    """Per-property permit lookup via ArcGIS API (county) and Accela CSV export (Tampa).

    Writes to PG tables county_permits / tampa_accela_records.
    """

    def __init__(self, *, pg_dsn: str | None = None, headless: bool = True):
        self._county = CountyPermitService(pg_dsn=pg_dsn)
        self._tampa = TampaPermitService(pg_dsn=pg_dsn, headless=headless)
        self._engine = get_engine(resolve_pg_dsn(pg_dsn))

    def _strap_to_folio(self, strap: str) -> str | None:
        """Look up 10-digit PG folio from strap via PG function."""
        with self._engine.connect() as conn:
            row = conn.execute(
                text("SELECT strap_to_folio(:strap)"),
                {"strap": strap},
            ).fetchone()
        return row[0] if row else None

    def fetch_county_by_folio(self, folio: str) -> dict:
        """Fetch county permits for a 10-digit folio via ArcGIS REST API.

        Converts 10-digit folio (e.g. ``0472200300``) to the dotted format
        used by ArcGIS PARCEL field (``047220.0300``).
        """
        if not folio or len(folio) != 10 or not folio.isdigit():
            logger.warning(f"Invalid folio for county permit lookup: {folio!r}")
            return {"seen": 0, "written": 0, "skipped_missing_permit": 0, "skipped_missing_object_id": 0}

        dotted = f"{folio[:6]}.{folio[6:]}"
        where = f"PARCEL = '{dotted}'"
        logger.info(f"County permit lookup: folio={folio} → PARCEL={dotted}")
        return self._county.sync_postgres(where=where, page_size=100)

    def fetch_tampa_by_address(self, address: str) -> dict:
        """Fetch Tampa Accela records by address via CSV export.

        Launches headless Chrome to query Accela global search and export CSV,
        then upserts rows into tampa_accela_records.
        """
        if not address or address.strip().lower() in ("unknown", "n/a", "none", ""):
            logger.warning(f"Invalid address for Tampa permit lookup: {address!r}")
            return {"parsed": 0, "written": 0}

        logger.info(f"Tampa permit lookup: address={address!r}")
        result = self._tampa.capture_query_export(address)

        if not result.csv_path or result.row_count == 0:
            logger.info(f"Tampa permit lookup: no records for {address!r}")
            return {"parsed": 0, "written": 0}

        stats = self._tampa.sync_csv_to_postgres(
            result.csv_path, source_query_text=address
        )
        Path(result.csv_path).unlink(missing_ok=True)
        logger.info(f"Tampa permit lookup: {stats}")
        return stats

    def fetch_all(
        self,
        *,
        folio: str | None = None,
        strap: str | None = None,
        address: str | None = None,
    ) -> dict:
        """Run both county (by folio) and Tampa (by address) lookups.

        Args:
            folio: 10-digit PG folio for county ArcGIS lookup.
            strap: HCPA strap (pipeline parcel_id) — resolved to folio via PG.
            address: Street address for Tampa Accela lookup.

        Returns combined stats dict. Errors in one source don't block the other.
        """
        county_stats = {}
        tampa_stats = {}

        # Resolve strap → folio if folio not provided directly
        if not folio and strap:
            folio = self._strap_to_folio(strap)
            if not folio:
                logger.warning(f"Could not resolve strap={strap} to folio — skipping county lookup")

        if folio:
            try:
                county_stats = self.fetch_county_by_folio(folio)
            except Exception as e:
                logger.error(f"County permit lookup failed for folio={folio}: {e}")
                county_stats = {"error": str(e)}

        if address:
            try:
                tampa_stats = self.fetch_tampa_by_address(address)
            except Exception as e:
                logger.error(f"Tampa permit lookup failed for address={address!r}: {e}")
                tampa_stats = {"error": str(e)}

        return {"county": county_stats, "tampa": tampa_stats}


def check_property_permits(
    folio: str | None = None,
    strap: str | None = None,
    address: str | None = None,
) -> dict:
    """CLI convenience: look up permits for a single property."""
    scraper = PermitScraper()
    return scraper.fetch_all(folio=folio, strap=strap, address=address)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Per-property permit lookup")
    parser.add_argument("--folio", help="10-digit folio (county ArcGIS lookup)")
    parser.add_argument("--strap", help="HCPA strap / pipeline parcel_id (resolved to folio via PG)")
    parser.add_argument("--address", help="Street address (Tampa Accela lookup)")
    args = parser.parse_args()

    if not args.folio and not args.strap and not args.address:
        parser.error("Provide at least --folio, --strap, or --address")

    scraper = PermitScraper(headless=True)
    result = scraper.fetch_all(folio=args.folio, strap=args.strap, address=args.address)
    print(result)
