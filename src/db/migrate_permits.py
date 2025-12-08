import duckdb
from loguru import logger

def migrate():
    db_path = "data/property_master.db"
    conn = duckdb.connect(db_path)
    
    try:
        # Create table if not exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS permits (
                id INTEGER PRIMARY KEY,
                folio VARCHAR,
                permit_number VARCHAR,
                issue_date DATE,
                status VARCHAR,
                permit_type VARCHAR,
                description VARCHAR,
                contractor VARCHAR,
                estimated_cost DECIMAL(12, 2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Add ID sequence if not exists
        try:
            conn.execute("CREATE SEQUENCE IF NOT EXISTS permits_id_seq")
            conn.execute("ALTER TABLE permits ALTER COLUMN id SET DEFAULT nextval('permits_id_seq')")
        except Exception:
            pass

        # Add new columns
        try:
            conn.execute("ALTER TABLE permits ADD COLUMN IF NOT EXISTS url VARCHAR")
            logger.info("Added url column")
        except Exception as e:
            logger.warning(f"Could not add url column: {e}")

        try:
            conn.execute("ALTER TABLE permits ADD COLUMN IF NOT EXISTS noc_instrument VARCHAR")
            logger.info("Added noc_instrument column")
        except Exception as e:
            logger.warning(f"Could not add noc_instrument column: {e}")
            
        logger.success("Permits table migration complete")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
