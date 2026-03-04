import asyncio
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.scripts.photo_metrics import get_market_photo_metrics  # noqa: E402
from src.services.market_data_service import MarketDataService  # noqa: E402


async def test():
    print("Metrics BEFORE:")
    print(json.dumps(get_market_photo_metrics(), indent=2))

    print("\nTesting market data service on 182936509000044000130A...")
    svc = MarketDataService(use_windows_chrome=True)

    # We pass the property as if from the foreclosures table
    res = await svc.run_batch([
        {"strap": "182936509000044000130A", "property_address": "8133 LAKE MEADOWS DR", "case_number": "2024-CA-001234"}
    ])

    print("\nBatch Result:")
    print(res)

    print("\nMetrics AFTER:")
    print(json.dumps(get_market_photo_metrics(), indent=2))


if __name__ == "__main__":
    asyncio.run(test())
