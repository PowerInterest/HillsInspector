import asyncio
from datetime import date
from src.history.scrape_history import HistoricalScraper
if(1==1):
  async def main():
      s = HistoricalScraper(max_concurrent=1)
      try:
          auctions = await s.scrape_single_date(date(2025, 12, 10))
          print('auctions', len(auctions))
          print('winning_bid_nonnull', sum(1 for a in auctions if a.get('winning_bid')))
          print('sold_to_nonnull', sum(1 for a in auctions if a.get('sold_to')))
      finally:
          await s.close_browser()

  asyncio.run(main())
