from loguru import logger
from sqlalchemy import create_engine, text
from sunbiz.db import resolve_pg_dsn


def main():
    engine = create_engine(resolve_pg_dsn())
    logger.info("Starting property_market photos backfill...")

    query = """
        WITH candidates AS (
            SELECT 
                strap,
                photo_cdn_urls,
                jsonb_array_length(CASE WHEN jsonb_typeof(photo_cdn_urls) = 'array' THEN photo_cdn_urls ELSE '[]'::jsonb END) as current_len,
                zillow_json->'photos' as z_photos,
                jsonb_array_length(CASE WHEN jsonb_typeof(zillow_json->'photos') = 'array' THEN zillow_json->'photos' ELSE '[]'::jsonb END) as z_len,
                homeharvest_json->'photos' as hh_photos,
                jsonb_array_length(CASE WHEN jsonb_typeof(homeharvest_json->'photos') = 'array' THEN homeharvest_json->'photos' ELSE '[]'::jsonb END) as hh_len,
                realtor_json->'photos' as r_photos,
                jsonb_array_length(CASE WHEN jsonb_typeof(realtor_json->'photos') = 'array' THEN realtor_json->'photos' ELSE '[]'::jsonb END) as r_len
            FROM property_market
        ),
        updates AS (
            SELECT 
                strap,
                CASE 
                    WHEN z_len >= hh_len AND z_len >= r_len AND z_len > current_len THEN z_photos
                    WHEN hh_len >= z_len AND hh_len >= r_len AND hh_len > current_len THEN hh_photos
                    WHEN r_len >= z_len AND r_len >= hh_len AND r_len > current_len THEN r_photos
                    ELSE NULL
                END as new_photos
            FROM candidates
        )
        UPDATE property_market pm
        SET photo_cdn_urls = u.new_photos,
            updated_at = NOW()
        FROM updates u
        WHERE pm.strap = u.strap 
          AND u.new_photos IS NOT NULL;
    """

    with engine.begin() as conn:
        result = conn.execute(text(query))
        logger.info(f"Backfilled photos for {result.rowcount} properties.")


if __name__ == "__main__":
    main()
