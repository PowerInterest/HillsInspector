import asyncio
import logging
from typing import Any, List, Optional
from pathlib import Path
from loguru import logger
from src.db.operations import PropertyDB

class DatabaseWriter:
    """
    Single-writer queue for DuckDB operations.
    
    This class manages an asyncio Queue where multiple scrapers/workers can push
    data write requests. A single background task pulls batches from the queue
    and executes them against the database, preventing concurrent write contention.
    """
    
    def __init__(self, db_path: Path = Path("data/property_master.db")):
        self.db_path = db_path
        self.queue = asyncio.Queue()
        self.running = False
        self._worker_task = None
        # We can perform writes via the existing PropertyDB logic
        # Since this worker runs sequentially, we avoid concurrency issues.
        self.db = PropertyDB(str(db_path))

    async def start(self):
        """Start the background writer worker."""
        if self.running:
            return
            
        self.running = True
        self._worker_task = asyncio.create_task(self._worker())
        logger.info("DatabaseWriter started.")

    async def stop(self):
        """Stop the background worker and flush remaining items."""
        logger.info("Stopping DatabaseWriter...")
        self.running = False
        if self._worker_task:
            await self.queue.join()  # Wait for queue to process
            # Force wake up the worker so it can exit
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        logger.info("DatabaseWriter stopped.")

    async def enqueue(self, operation: str, data: Any):
        """
        Push a write operation to the queue.
        
        Args:
            operation: String identifier for the operation (e.g., 'upsert_parcel')
            data: The data object/dict required for the operation.
        """
        await self.queue.put((operation, data))

    async def _worker(self):
        """Background loop to process queue items."""
        while self.running or not self.queue.empty():
            try:
                # Get a batch of items (up to 50 at a time or simple 1 by 1 for now)
                # For simplicity in V1, we process 1 by 1 but fast
                # Ideally we batch them.
                
                # Wait for next item
                try:
                    # Timeout allows checking self.running periodically
                    item = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                operation, data = item
                
                try:
                    self._execute_write(operation, data)
                except Exception as e:
                    logger.error(f"DB Write Error ({operation}): {e}")
                finally:
                    self.queue.task_done()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"DatabaseWriter worker crash: {e}")
                await asyncio.sleep(1) # Prevent tight loop on crash

    def _execute_write(self, operation: str, data: Any):
        """Dispatch write operation to PropertyDB methods."""
        
        if operation == "upsert_auction":
            self.db.upsert_auction(data)
            
        elif operation == "upsert_parcel":
            self.db.upsert_parcel(data)
            
        elif operation == "save_market_data":
            # Expects dict with: folio, source, data, screenshot_path
            self.db.save_market_data(
                folio=data["folio"],
                source=data["source"],
                data=data["data"],
                screenshot_path=data.get("screenshot_path")
            )
            
        elif operation == "save_liens":
            # Expects dict with: folio, liens_list
            self.db.save_liens(data["folio"], data["liens"])
            
        elif operation == "save_permits":
            # Expects dict with: folio, permits_list (Permit objects or dicts)
            folio = data["folio"]
            permits = data["permits"]
            self.db.save_permits(folio, permits)
            
        elif operation == "save_flood_data":
             self.db.save_flood_data(
                folio=data["folio"],
                flood_zone=data["flood_zone"],
                flood_risk=data["flood_risk"],
                insurance_required=data["insurance_required"]
            )
            
        elif operation == "update_step_status":
             # Generic step completion mark
             self.db.mark_step_complete(data["case_number"], data["step_column"])
             
        else:
            logger.warning(f"Unknown DB operation: {operation}")
