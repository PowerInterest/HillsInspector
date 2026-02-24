import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        
        # Pre-navigate to get cookies
        await page.goto("https://publicaccess.hillsclerk.com/Public/ORIUtilities/")

        res = await page.evaluate("""async () => {
            const r = await fetch('https://publicaccess.hillsclerk.com/Public/ORIUtilities/DocumentSearch/api/Search', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({"Instrument": "2022527083"})
            });
            return await r.json();
        }""")
        print("Instrument Payload:", res)

        res2 = await page.evaluate("""async () => {
            const r = await fetch('https://publicaccess.hillsclerk.com/Public/ORIUtilities/DocumentSearch/api/Search', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({"InstrumentNumber": "2022527083"})
            });
            return await r.json();
        }""")
        print("InstrumentNumber Payload:", res2)

        res3 = await page.evaluate("""async () => {
            const r = await fetch('https://publicaccess.hillsclerk.com/Public/ORIUtilities/DocumentSearch/api/Search', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({"InstrumentNum": "2022527083"})
            });
            return await r.json();
        }""")
        print("InstrumentNum Payload:", res3)
        
        await browser.close()
        
asyncio.run(main())
