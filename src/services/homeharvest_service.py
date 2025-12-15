import json
import time
import random
from datetime import datetime, date
from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger
from homeharvest import scrape_property
from src.db.operations import PropertyDB

class HomeHarvestService:
    def __init__(self):
        self.db = PropertyDB()

    def get_pending_properties(self, limit: int = 100, auction_date: Optional[date] = None) -> List[Dict[str, Any]]:
        """
        Get properties that need HomeHarvest enrichment.
        Criteria: Has address, is not marked as needing HCPA review, and no recent HomeHarvest record.
        """
        conn = self.db.connect()
        
        query = """
            SELECT DISTINCT
                a.folio, 
                p.property_address, 
                p.city, 
                p.zip_code,
                a.case_number
            FROM auctions a
            INNER JOIN parcels p ON a.folio = p.folio
            LEFT JOIN home_harvest h ON a.folio = h.folio
            WHERE a.property_address IS NOT NULL 
              AND a.property_address != ''
              AND (a.hcpa_scrape_failed IS NULL OR a.hcpa_scrape_failed = FALSE) -- Only good properties
              AND (h.folio IS NULL OR h.created_at < CURRENT_DATE - INTERVAL 7 DAY) -- No recent data
              AND a.needs_homeharvest_enrichment = TRUE -- Flag to be enriched
        """
        params = []

        if auction_date:
            query += " AND a.auction_date = ?"
            params.append(auction_date)

        query += " LIMIT ?"
        params.append(limit)

        results = conn.execute(query, params).fetchall()
        
        props = []
        for r in results:
            addr = r[1].strip()
            city = r[2].strip() if r[2] else ""
            zip_c = r[3].strip() if r[3] else ""
            state = "FL"
            
            location = f"{addr}, {city}, {state} {zip_c}".strip()
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

        locations = [p["location"] for p in properties]
        folio_map = {p["location"]: p["folio"] for p in properties}
        
        logger.info(f"Fetching HomeHarvest data for {len(locations)} properties...")
        
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
                self._process_single_property(prop["folio"], prop["location"])
                
                # Add delay between requests to avoid rate limiting
                # Skip delay after the last one
                if i < len(properties) - 1:
                    delay = random.uniform(3.0, 7.0)
                    logger.debug(f"Sleeping {delay:.1f}s...")
                    time.sleep(delay)
                
        except Exception as e:
            logger.error(f"HomeHarvest batch error: {e}")

    def _process_single_property(self, folio: str, location: str):
        try:
            logger.info(f"Scraping: {location}")
            # We search for "sold" to get history/metadata. 
            df = scrape_property(location=location, listing_type="sold", past_days=3650) # 10 years
            
            if df is None or df.empty:
                logger.warning(f"No data found for {location}")
                # Try 'for_sale' just in case it's currently active and not sold recently?
                # df = scrape_property(location=location, listing_type="for_sale")
                return

            # Take the most recent relevant record (usually the first one)
            # HomeHarvest returns a DataFrame.
            row = df.iloc[0]
            self._save_record(folio, row)
            logger.success(f"Saved data for {folio}")
            
        except Exception as e:
            logger.error(f"Error processing {location}: {e}")

    def _save_record(self, folio: str, row: pd.Series):
        conn = self.db.connect()
        
        # Helper to get value safely
        def val(col, dtype=str):
            if col not in row: return None
            v = row[col]
            if pd.isna(v): return None
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
                # pandas timestamp to string
                return pd.to_datetime(v).isoformat()
            except Exception:
                return str(v)

        # Map fields
        data = {
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
            'primary_photo': val('primary_photo'),
            'alt_photos': val('alt_photos', 'json'),
        }

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
