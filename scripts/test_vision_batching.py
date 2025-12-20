import asyncio
import time
import sys
from loguru import logger

# Add project root to path
sys.path.append('.')

from src.services.vision_service import VisionService

def simulated_vision_task(idx, duration=1.0):
    """Simulate a blocking vision task."""
    logger.info(f"Task {idx} started")
    time.sleep(duration)
    logger.info(f"Task {idx} finished")
    return idx

async def test_batching():
    logger.info("Initializing VisionService...")
    service = VisionService()
    
    # Check if semaphore exists
    if not hasattr(service, '_semaphore'):
        logger.error("VisionService has no _semaphore!")
        return

    logger.info(f"Semaphore limit: 1 (implied by class logic)")
    
    start_time = time.time()
    
    async with asyncio.TaskGroup() as tg:
        for i in range(3):
            tg.create_task(service.process_async(simulated_vision_task, i, 1.0))
            
    total_time = time.time() - start_time
    logger.info(f"Total time for 3 tasks (1s each): {total_time:.2f}s")
    
    if total_time >= 3.0:
        logger.success("SUCCESS: Tasks executed sequentially (or constrained). Batching is working.")
    else:
        logger.error("FAILURE: Tasks executed too quickly! Semaphore might be broken.")

if __name__ == "__main__":
    asyncio.run(test_batching())
