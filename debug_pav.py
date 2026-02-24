import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        res = await page.evaluate("""async () => {
            const payload = {
                "QueryID": "108",
                "Keywords": [{"KeywordName": "1006", "KeywordValue": "2022527083"}],
                "MaxRows": 25,
                "SortDir": "desc",
                "SortField": "RecDate"
            };
            const r = await fetch('https://publicaccess.hillsclerk.com/PAVDirectSearch/api/CustomQuery/KeywordSearch', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(payload)
            });
            return await r.json();
        }""")
        print("1006:", res)

        res2 = await page.evaluate("""async () => {
            const payload = {
                "QueryID": "108",
                "Keywords": [{"KeywordName": "InstrumentNumber", "KeywordValue": "2022527083"}],
                "MaxRows": 25,
                "SortDir": "desc",
                "SortField": "RecDate"
            };
            const r = await fetch('https://publicaccess.hillsclerk.com/PAVDirectSearch/api/CustomQuery/KeywordSearch', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(payload)
            });
            return await r.json();
        }""")
        print("InstrumentNumber:", res2)
        
        await browser.close()
        
asyncio.run(main())
