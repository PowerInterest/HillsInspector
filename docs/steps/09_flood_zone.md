# FEMA Flood Scraper

## Overview
The `FEMAFloodChecker` queries the FEMA National Flood Hazard Layer (NFHL) ArcGIS API to determine the flood zone for a given property.

## Source
- **URL**: `https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer`
- **Type**: API (ArcGIS REST)

## Inputs
- **Coordinates**: Latitude and Longitude of the property.
- **Property ID**: For caching purposes.

## Outputs
- **FloodZoneResult**: Object containing:
    - Flood Zone (e.g., AE, X, VE)
    - Risk Level (High, Moderate, Minimal)
    - Base Flood Elevation (BFE)
    - Insurance Requirement status
- **Files Stored via ScraperStorage**:
    - **Raw Data**: JSON response from FEMA API saved to `data/properties/{property_id}/raw/fema/flood_zone.json`

## Key Methods
- `get_flood_zone(lat, lon)`: Queries the API for a specific coordinate.
- `get_flood_zone_for_property(property_id, lat, lon)`: Wrapper with caching support via `ScraperStorage`.
