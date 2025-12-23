import asyncio
import contextlib
from pathlib import Path
from typing import Any
from loguru import logger
from src.db.operations import PropertyDB

class DatabaseWriter:
    """
    Single-writer queue for DuckDB operations.
    
    This class manages an asyncio Queue where multiple scrapers/workers can push
    data write requests. A single background task pulls batches from the queue
    and executes them against the database, preventing concurrent write contention.
    """
    
    def __init__(self, db_path: Path = Path("data/property_master.db"), db: PropertyDB | None = None):
        self.db_path = Path(db.db_path) if db else db_path
        self.queue = asyncio.Queue()
        self.running = False
        self._worker_task = None
        # We can perform writes via the existing PropertyDB logic
        # Since this worker runs sequentially, we avoid concurrency issues.
        self.db = db or PropertyDB(str(self.db_path))

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
            with contextlib.suppress(asyncio.CancelledError):
                await self._worker_task
        logger.info("DatabaseWriter stopped.")

    async def enqueue(self, operation: str, data: Any):
        """
        Push a write operation to the queue.
        
        Args:
            operation: String identifier for the operation (e.g., 'upsert_parcel')
            data: The data object/dict required for the operation.
        """
        # Internal: Enqueue with no future
        await self.queue.put((operation, data, None))

    async def execute_with_result(self, func, *args, **kwargs) -> Any:
        """
        Execute a DB function sequentially via the writer queue and return the result.
        
        Useful for operations that read/write and need return values (e.g., IDs).
        
        Args:
            func: The callable to execute (e.g., db.save_document)
            *args: Positional arguments for func
            **kwargs: Keyword arguments for func
            
        Returns:
            The result of func(*args, **kwargs)
        """
        future = asyncio.get_running_loop().create_future()
        data = {"func": func, "args": args, "kwargs": kwargs}
        await self.queue.put(("generic_call", data, future))
        return await future

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
                except TimeoutError:
                    continue

                operation, data, future = item
                
                try:
                    result = self._execute_write(operation, data)
                    if future and not future.done():
                        future.set_result(result)
                except Exception as e:
                    logger.error(f"DB Write Error ({operation}): {e}")
                    if future and not future.done():
                        future.set_exception(e)
                finally:
                    self.queue.task_done()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"DatabaseWriter worker crash: {e}")
                await asyncio.sleep(1) # Prevent tight loop on crash

    def _execute_write(self, operation: str, data: Any) -> Any:
        """Dispatch write operation to PropertyDB methods."""
        
        if operation == "generic_call":
            func = data["func"]
            args = data.get("args", [])
            kwargs = data.get("kwargs", {})
            # If a PropertyDB method from a different instance was passed in,
            # rebind to the writer's DB to keep all writes serialized.
            target = getattr(func, "__self__", None)
            if isinstance(target, PropertyDB):
                method_name = getattr(func, "__name__", None)
                if method_name and hasattr(self.db, method_name):
                    return getattr(self.db, method_name)(*args, **kwargs)
            return func(*args, **kwargs)

        if operation == "upsert_auction":
            self.db.upsert_auction(data)
            
        elif operation == "upsert_parcel":
            # Fix: upsert_parcel expects Property object, but enqueue sends dict
            from src.models.property import Property
            prop = Property(**data) if isinstance(data, dict) else data
            self.db.upsert_parcel(prop)
            
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

        elif operation == "update_tax_status":
            self.db.update_parcel_tax_status(data["folio"], data["tax_status"], data["tax_warrant"])
             
        else:
            logger.warning(f"Unknown DB operation: {operation}")
            return None
        return None
