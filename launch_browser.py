from playwright.sync_api import sync_playwright
from playwright_stealth.stealth import Stealth

print("Launching browser...")

stealth = Stealth()
playwright_manager = sync_playwright()

# The use_sync method wraps the playwright context manager
with stealth.use_sync(playwright_manager) as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()

    page.goto('https://bot.sannysoft.com/')

    print("\nBrowser is running. Inspect it now.")
    print("Press Enter in this terminal to close the browser...")
    input() # This will pause the script, keeping the browser open

    browser.close()
    print("Browser closed.")

