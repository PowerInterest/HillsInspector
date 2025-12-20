import asyncio
import logging
import sys
import os
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.orchestrator import PipelineOrchestrator
from src.db.writer import DatabaseWriter
from src.db.operations import PropertyDB
from src.models.property import Property

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_orchestrator")

async def main():
    logger.info("Starting Orchestrator Verification...")
    
    # Setup isolated DB
    original_db = "data/property_master.db"
    test_db = "data/test_orchestrator.duckdb"
    
    if os.path.exists(test_db):
        os.remove(test_db)
    
    if os.path.exists(original_db):
        logger.info(f"Copying {original_db} to {test_db}")
        shutil.copy(original_db, test_db)
    else:
        logger.warning(f"Original DB {original_db} not found. Creating empty test DB.")
        
    # Override Env Var
    os.environ["HILLS_DB_PATH"] = test_db
    
    try:
        # Initialize DB (Test Instance)
        db = PropertyDB()
        conn = db.connect()
        
        # Find a suitable date
        rows = conn.execute("""
            SELECT auction_date, COUNT(*) as c 
            FROM auctions 
            WHERE parcel_id IS NOT NULL
            GROUP BY auction_date 
            HAVING c >= 1 
            ORDER BY c ASC 
            LIMIT 1
        """).fetchall()
        
        if not rows:
            target_date = date.today()
            logger.warning(f"No suitable batch found. Defaulting to today: {target_date}")
        else:
            target_date_str = rows[0][0]
            if isinstance(target_date_str, str):
                 target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
            else:
                 target_date = target_date_str
            logger.info(f"Test Date: {target_date} ({rows[0][1]} auctions)")
        
        # Close connection explicitly
        conn.close()
        
        # Re-initialize DB for Orchestrator to ensure fresh connection
        db = PropertyDB()

        # Initialize Orchestrator
        # Writer uses the same env var because it uses PropertyDB internally? 
        # Writer takes PropertyDB instance.
        writer = DatabaseWriter(db)
        orchestrator = PipelineOrchestrator(db_writer=writer, db=db)
        
        # Start Writer
        await writer.start()
        worker_task = writer._worker_task
        
        # Run Orchestrator
        logger.info(f"Executing pipeline for {target_date}...")
        await orchestrator.process_auctions(target_date, target_date)
        
        logger.info("Orchestrator finished.")
        
        # Verify Results (Optional)
        # conn = db.connect() 
        # ... check status ...
        # conn.close()

    except Exception as e:
        logger.exception("Test failed.")
        raise
    finally:
        # Stop Writer
        if 'writer' in locals():
            await writer.stop()
            
        # Cleanup
        if os.path.exists(test_db):
            logger.info("Cleaning up test DB.")
            try:
                os.remove(test_db)
            except: pass

if __name__ == "__main__":
    asyncio.run(main())
