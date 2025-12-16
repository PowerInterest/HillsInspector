from src.services.homeharvest_service import HomeHarvestService
from src.db.operations import PropertyDB
from loguru import logger
from datetime import date
import re

def enrich_dec15_auctions():
    """
    Directly enrich properties for the Dec 15, 2025 auction.
    """
    target_date = date(2025, 12, 15)
    db = PropertyDB()
    hh_service = HomeHarvestService()
    
    logger.info(f"Finding properties for auction on {target_date}...")
    
    conn = db.connect()
    query = """
        SELECT 
            a.folio, 
            p.property_address, 
            p.city, 
            p.zip_code
        FROM auctions a
        JOIN parcels p ON a.folio = p.folio
        WHERE a.auction_date = ?
          AND p.property_address IS NOT NULL 
          AND p.property_address != ''
    """
    
    results = conn.execute(query, [target_date]).fetchall()
    
    if not results:
        logger.warning(f"No properties found for {target_date} with valid addresses.")
        return

    logger.info(f"Found {len(results)} properties. Preparing for enrichment...")
    
    props_to_enrich = []
    for r in results:
        raw_addr = r[1].strip()
        # Fix HCPA format "CITY, FL- ZIP" -> "CITY, FL ZIP"
        clean_addr = raw_addr.replace("FL- ", "FL ").replace("  ", " ")
        
        # Check if address already looks complete (ends with FL + Zip)
        if re.search(r'FL\s+\d{5}', clean_addr):
            location = clean_addr
        else:
            city = r[2].strip() if r[2] else "Tampa"
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
                
            location = ", ".join(parts).replace("FL, ", "FL ")
        
        props_to_enrich.append({
            "folio": r[0],
            "location": location
        })
        logger.info(f"  Target: {location} (Folio: {r[0]})")

    # Run enrichment
    hh_service.fetch_and_save(props_to_enrich)
    logger.success("Enrichment complete.")

if __name__ == "__main__":
    enrich_dec15_auctions()
