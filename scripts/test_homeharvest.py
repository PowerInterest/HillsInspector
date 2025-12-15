import duckdb
from homeharvest import scrape_property

def get_test_property():
    """Fetch a single property address from the database."""
    conn = duckdb.connect('data/property_master.db')
    
    # Get a property that has an address
    query = """
    SELECT property_address, city, zip_code 
    FROM parcels 
    WHERE property_address IS NOT NULL 
    AND property_address != ''
    LIMIT 1
    """
    
    try:
        row = conn.execute(query).fetchone()
        if row:
            # Construct a clean address string
            addr = row[0].strip()
            city = row[1].strip() if row[1] else "Tampa"
            zip_code = row[2].strip() if row[2] else ""
            
            return f"{addr}, {city}, FL {zip_code}".strip()
    except Exception as e:
        print(f"Database error: {e}")
    finally:
        conn.close()
    return None

def test_homeharvest():
    # print(f"HomeHarvest Version: {homeharvest.__version__}")
    location = get_test_property()
    if not location:
        print("No suitable test property found in database.")
        location = "3006 W Julia St, Tampa, FL 33629"
        
    print(f"Testing HomeHarvest for: {location}")
    print("-" * 50)

    try:
        # listing_type: for_sale, for_rent, sold
        # We probably want 'sold' for comps or 'for_sale' if active
        # Let's try to get everything for this specific address
        
        properties = scrape_property(
            location=location,
            listing_type="sold", # Look for past sales (comps logic)
            past_days=3000 # Go back far enough to find the last sale
        )
        
        print(f"Found {len(properties)} records.")
        
        if not properties.empty:
            # Display key columns
            cols = [
                'property_url', 'mls', 'status', 'style', 
                'beds', 'full_baths', 'half_baths', 'sqft', 
                'year_built', 'days_on_mls', 'list_price', 
                'list_date', 'sold_price', 'last_sold_date', 
                'lot_sqft', 'price_per_sqft', 'hoa_fee', 
                'parking_garage'
            ]
            
            # Filter cols that actually exist in the dataframe
            existing_cols = [c for c in cols if c in properties.columns]
            
            # Transpose for easier reading of a single record
            print(properties[existing_cols].iloc[0].to_string())
            
            # Print raw columns to see what else is available
            print("\nAll Available Columns:")
            print(properties.columns.tolist())
            
    except Exception as e:
        print(f"HomeHarvest Error: {e}")

if __name__ == "__main__":
    test_homeharvest()
