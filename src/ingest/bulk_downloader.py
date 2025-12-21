
import re
import asyncio
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright
from loguru import logger

HCPA_DOWNLOADS_URL = "https://downloads.hcpafl.org/"
DATA_DIR = Path("data")
BULK_DATA_DIR = DATA_DIR / "bulk_data"

async def download_latest_bulk_data(output_dir: Path = BULK_DATA_DIR) -> dict:
    """
    Download the latest 'parcel_*.zip' and 'LatLon_*.zip' files from HCPA.
    
    Returns:
        Dictionary with paths to the downloaded files:
        {
            "parcel_zip": Path(...),
            "latlon_zip": Path(...)
        }
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    results = {}

    async with async_playwright() as p:
        # Use headless chrome
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        try:
            logger.info(f"Navigating to {HCPA_DOWNLOADS_URL}...")
            await page.goto(HCPA_DOWNLOADS_URL)
            await page.wait_for_load_state("networkidle")

            # Get all links
            links = await page.locator("a").all()
            
            parcel_files = []
            latlon_files = []
            
            # Regex to match filenames like parcel_12_19_2025.zip
            # Pattern: matches start, date parts, and extension
            # Group 1: date string (e.g. 12_19_2025)
            parcel_pattern = re.compile(r"parcel_(\d{2}_\d{2}_\d{4})\.zip", re.IGNORECASE)
            latlon_pattern = re.compile(r"LatLon_Table_(\d{2}_\d{2}_\d{4})\.zip", re.IGNORECASE)

            for link in links:
                text = await link.inner_text()
                text = text.strip()
                
                # Check for parcel file
                p_match = parcel_pattern.search(text)
                if p_match:
                    date_str = p_match.group(1)
                    date_obj = datetime.strptime(date_str, "%m_%d_%Y")
                    parcel_files.append({"link": link, "date": date_obj, "filename": text})
                    continue

                # Check for LatLon file
                l_match = latlon_pattern.search(text)
                if l_match:
                    date_str = l_match.group(1)
                    date_obj = datetime.strptime(date_str, "%m_%d_%Y")
                    latlon_files.append({"link": link, "date": date_obj, "filename": text})

            # Sort by date descending and get latest
            if not parcel_files:
                logger.error("No parcel_*.zip files found!")
            else:
                latest_parcel = sorted(parcel_files, key=lambda x: x["date"], reverse=True)[0]
                logger.info(f"Latest Parcel File: {latest_parcel['filename']}")
                results["parcel_zip"] = await _download_file(latest_parcel["link"], output_dir, latest_parcel["filename"])

            if not latlon_files:
                logger.error("No LatLon_*.zip files found!")
            else:
                latest_latlon = sorted(latlon_files, key=lambda x: x["date"], reverse=True)[0]
                logger.info(f"Latest LatLon File: {latest_latlon['filename']}")
                results["latlon_zip"] = await _download_file(latest_latlon["link"], output_dir, latest_latlon["filename"])

        finally:
            await browser.close()
            
    return results

async def _download_file(link_locator, output_dir: Path, filename: str) -> Path:
    """Helper to click a link and handle the download."""
    target_path = output_dir / filename
    
    if target_path.exists():
        logger.info(f"File already exists: {target_path}, skipping download.")
        return target_path

    logger.info(f"Downloading {filename}...")
    
    # Setup download listener
    async with link_locator.page.expect_download() as download_info:
        # Click the link (this triggers the postback and download)
        # We might need force=True if it looks non-interactive, but usually standard click works
        await link_locator.click()

    download = await download_info.value
    await download.save_as(target_path)
    
    logger.success(f"Downloaded: {target_path} ({target_path.stat().st_size / 1024 / 1024:.1f} MB)")
    return target_path

if __name__ == "__main__":
    import sys
    # Add project root to path so we can run as script
    sys.path.append(".")
    
    try:
        downloads = asyncio.run(download_latest_bulk_data())
        print(f"Downloads complete: {downloads}")
    except Exception as e:
        logger.error(f"Download failed: {e}")
        sys.exit(1)
