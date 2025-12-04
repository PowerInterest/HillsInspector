"""
FEMA Flood Zone lookup using the National Flood Hazard Layer (NFHL) ArcGIS API.

This uses FEMA's public ArcGIS REST API to query flood zone information
by latitude/longitude coordinates.

API Documentation:
- NFHL MapServer: https://hazards.fema.gov/gis/nfhl/rest/services/public/NFHL/MapServer
- Layer 28: Flood Hazard Zones (S_FLD_HAZ_AR)

Flood Zone Definitions:
- Zone A: 1% annual chance flood, no BFE determined
- Zone AE: 1% annual chance flood, BFE determined (high risk)
- Zone AH: 1% annual chance flood, ponding, 1-3 feet depth
- Zone AO: 1% annual chance flood, sheet flow
- Zone AR: Area with restoration underway
- Zone V/VE: Coastal flood zone with wave action (high risk)
- Zone X: 0.2% annual chance flood (minimal risk)
- Zone D: Undetermined risk

Usage:
    checker = FEMAFloodChecker()
    result = checker.get_flood_zone(lat=27.9506, lon=-82.4572)
    result = await checker.get_flood_zone_by_address("123 Main St, Tampa, FL")
"""

import requests
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any, List
from loguru import logger

from src.services.scraper_storage import ScraperStorage


@dataclass
class FloodZoneResult:
    """FEMA flood zone information for a location."""
    flood_zone: str  # e.g., "AE", "X", "VE"
    zone_subtype: Optional[str] = None  # Additional zone classification
    panel_number: Optional[str] = None  # FIRM panel number
    community_name: Optional[str] = None
    static_bfe: Optional[float] = None  # Base Flood Elevation if available
    is_high_risk: bool = False  # True for A/V zones
    is_moderate_risk: bool = False  # True for X (shaded)
    is_minimal_risk: bool = False  # True for X (unshaded)
    raw_response: Optional[Dict] = None

    @property
    def risk_level(self) -> str:
        """Human-readable risk level."""
        if self.is_high_risk:
            return "HIGH RISK - Special Flood Hazard Area (SFHA)"
        elif self.is_moderate_risk:
            return "MODERATE RISK - 0.2% annual chance flood"
        elif self.is_minimal_risk:
            return "MINIMAL RISK - Outside flood hazard area"
        return "UNDETERMINED"

    @property
    def insurance_required(self) -> bool:
        """Whether flood insurance is typically required for federally-backed mortgages."""
        return self.is_high_risk


class FEMAFloodChecker:
    """
    Query FEMA National Flood Hazard Layer for flood zone information.

    Uses the public ArcGIS REST API - no authentication required.
    """

    # FEMA NFHL MapServer endpoint - corrected URL
    # See: https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer
    BASE_URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer"

    # Layer IDs - Layer 28 is S_FLD_HAZ_AR (Flood Hazard Areas)
    FLOOD_HAZARD_ZONES_LAYER = 28  # S_FLD_HAZ_AR - Flood Hazard Areas
    FIRM_PANELS_LAYER = 3  # S_FIRM_PAN - FIRM Panels

    # High-risk zones (Special Flood Hazard Areas)
    HIGH_RISK_ZONES = {"A", "AE", "A1-30", "A99", "AH", "AO", "AR", "V", "VE", "V1-30"}

    # Moderate risk zones
    MODERATE_RISK_ZONES = {"X"}  # When ZONE_SUBTY is "0.2 PCT ANNUAL CHANCE FLOOD HAZARD"

    def __init__(self, timeout: int = 30, storage: Optional[ScraperStorage] = None):
        self.timeout = timeout
        self.session = requests.Session()
        self.storage = storage or ScraperStorage()

    def get_flood_zone(self, lat: float, lon: float) -> Optional[FloodZoneResult]:
        """
        Get flood zone information for a specific coordinate.

        Args:
            lat: Latitude (decimal degrees)
            lon: Longitude (decimal degrees)

        Returns:
            FloodZoneResult with flood zone information, or None if query failed
        """
        logger.info(f"Querying FEMA flood zone for coordinates: {lat}, {lon}")

        # Query parameters for point intersection
        # Note: FEMA NFHL uses NAD83 (EPSG:4269), not WGS84 (EPSG:4326)
        # Use esriSpatialRelIntersects for point-in-polygon queries
        params = {
            "where": "1=1",
            "geometry": f"{lon},{lat}",  # ArcGIS uses lon,lat order
            "geometryType": "esriGeometryPoint",
            "inSR": "4269",  # NAD83 coordinate system (FEMA standard)
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "FLD_ZONE,ZONE_SUBTY,SFHA_TF,STATIC_BFE",
            "returnGeometry": "false",
            "f": "json"
        }

        url = f"{self.BASE_URL}/{self.FLOOD_HAZARD_ZONES_LAYER}/query"

        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()

            if "error" in data:
                logger.error(f"FEMA API error: {data['error']}")
                return None

            features = data.get("features", [])

            if not features:
                logger.info(f"No flood zone data found for {lat}, {lon}")
                # Return minimal risk if no data (likely outside mapped area)
                return FloodZoneResult(
                    flood_zone="X",
                    is_minimal_risk=True,
                    raw_response=data
                )

            # Get the first (most relevant) result
            attrs = features[0].get("attributes", {})

            zone = attrs.get("FLD_ZONE", "UNKNOWN")
            zone_subtype = attrs.get("ZONE_SUBTY", "")
            sfha = attrs.get("SFHA_TF", "")  # "T" for True (in SFHA)
            static_bfe = attrs.get("STATIC_BFE")

            # Determine risk level
            is_high_risk = zone in self.HIGH_RISK_ZONES or sfha == "T"
            is_moderate_risk = (
                zone == "X" and
                zone_subtype and
                "0.2" in zone_subtype.upper()
            )
            is_minimal_risk = zone == "X" and not is_moderate_risk

            result = FloodZoneResult(
                flood_zone=zone,
                zone_subtype=zone_subtype if zone_subtype else None,
                static_bfe=float(static_bfe) if static_bfe and static_bfe > 0 else None,
                is_high_risk=is_high_risk,
                is_moderate_risk=is_moderate_risk,
                is_minimal_risk=is_minimal_risk,
                raw_response=data
            )

            logger.info(f"Flood zone result: {zone} ({result.risk_level})")
            return result

        except requests.RequestException as e:
            logger.error(f"FEMA API request failed: {e}")
            return None
        except (KeyError, ValueError) as e:
            logger.error(f"Error parsing FEMA response: {e}")
            return None

    def get_flood_zone_for_property(
        self,
        property_id: str,
        lat: float,
        lon: float,
        force_refresh: bool = False
    ) -> Optional[FloodZoneResult]:
        """
        Get flood zone for a property with caching.

        Args:
            property_id: Property folio/ID
            lat: Latitude
            lon: Longitude
            force_refresh: Force re-query even if cached

        Returns:
            FloodZoneResult or None
        """
        # Check if we have cached data
        if not force_refresh and not self.storage.needs_refresh(property_id, "fema", max_age_days=30):
            cached = self.storage.get_latest(property_id, "fema")
            if cached and cached.raw_data_path:
                raw_data = self.storage.load_vision_output(property_id, cached.raw_data_path.replace("raw/", "vision/")) or {}
                if raw_data.get("data"):
                    logger.debug(f"Using cached FEMA data for {property_id}")
                    data = raw_data.get("data", {})
                    return FloodZoneResult(
                        flood_zone=data.get("flood_zone", "UNKNOWN"),
                        zone_subtype=data.get("zone_subtype"),
                        static_bfe=data.get("static_bfe"),
                        is_high_risk=data.get("is_high_risk", False),
                        is_moderate_risk=data.get("is_moderate_risk", False),
                        is_minimal_risk=data.get("is_minimal_risk", False),
                        raw_response=data.get("raw_response")
                    )

        # Query FEMA API
        result = self.get_flood_zone(lat, lon)

        if result:
            # Convert to dict for storage
            result_dict = {
                "flood_zone": result.flood_zone,
                "zone_subtype": result.zone_subtype,
                "panel_number": result.panel_number,
                "community_name": result.community_name,
                "static_bfe": result.static_bfe,
                "is_high_risk": result.is_high_risk,
                "is_moderate_risk": result.is_moderate_risk,
                "is_minimal_risk": result.is_minimal_risk,
                "risk_level": result.risk_level,
                "insurance_required": result.insurance_required,
                "raw_response": result.raw_response,
                "coordinates": {"lat": lat, "lon": lon}
            }

            # Save raw API response
            raw_path = self.storage.save_raw_data(
                property_id=property_id,
                scraper="fema",
                data=result_dict,
                context="flood_zone"
            )

            # Record in database
            self.storage.record_scrape(
                property_id=property_id,
                scraper="fema",
                raw_data_path=raw_path,
                vision_data=result_dict,
                success=True
            )

            logger.info(f"Saved FEMA flood data for {property_id}: Zone {result.flood_zone}")

        return result

    def get_flood_zones_bulk(self, coordinates: List[tuple]) -> Dict[tuple, FloodZoneResult]:
        """
        Get flood zones for multiple coordinates.

        Args:
            coordinates: List of (lat, lon) tuples

        Returns:
            Dictionary mapping coordinates to FloodZoneResult
        """
        results = {}
        for lat, lon in coordinates:
            result = self.get_flood_zone(lat, lon)
            results[(lat, lon)] = result
        return results


class FloodZoneEnricher:
    """
    Enriches property data with flood zone information.
    Requires geocoded coordinates (lat/lon).
    """

    def __init__(self):
        self.checker = FEMAFloodChecker()

    def enrich_property(self, property_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add flood zone information to a property dictionary.

        Property must have 'latitude' and 'longitude' keys, or 'lat'/'lon'.

        Args:
            property_data: Dictionary with property information

        Returns:
            Property data with added flood zone fields
        """
        lat = property_data.get("latitude") or property_data.get("lat")
        lon = property_data.get("longitude") or property_data.get("lon")

        if not lat or not lon:
            logger.warning("Property missing coordinates, cannot determine flood zone")
            property_data["flood_zone"] = None
            property_data["flood_risk_level"] = "UNKNOWN - No coordinates"
            return property_data

        result = self.checker.get_flood_zone(float(lat), float(lon))

        if result:
            property_data["flood_zone"] = result.flood_zone
            property_data["flood_zone_subtype"] = result.zone_subtype
            property_data["flood_risk_level"] = result.risk_level
            property_data["flood_insurance_required"] = result.insurance_required
            property_data["flood_base_elevation"] = result.static_bfe
        else:
            property_data["flood_zone"] = None
            property_data["flood_risk_level"] = "QUERY FAILED"

        return property_data


# Tampa area test coordinates
TAMPA_TEST_COORDS = [
    (27.9506, -82.4572),  # Downtown Tampa
    (28.0392, -82.4675),  # Carrollwood
    (27.8419, -82.7903),  # Beach area
]


if __name__ == "__main__":
    checker = FEMAFloodChecker()

    print("=== FEMA Flood Zone Lookup ===\n")

    for lat, lon in TAMPA_TEST_COORDS:
        print(f"Coordinates: {lat}, {lon}")
        result = checker.get_flood_zone(lat, lon)
        if result:
            print(f"  Zone: {result.flood_zone}")
            print(f"  Subtype: {result.zone_subtype or 'N/A'}")
            print(f"  Risk Level: {result.risk_level}")
            print(f"  Insurance Required: {result.insurance_required}")
            if result.static_bfe:
                print(f"  Base Flood Elevation: {result.static_bfe} ft")
        else:
            print("  Failed to get flood zone data")
        print()
