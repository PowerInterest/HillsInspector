import asyncio
import sys
import json
import sqlite3
from pathlib import Path
from loguru import logger
from datetime import datetime

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.scrapers.ori_api_scraper import ORIApiScraper
from src.services.final_judgment_processor import FinalJudgmentProcessor
from src.history.db_init import ensure_history_schema
from src.db.sqlite_paths import resolve_sqlite_db_path_str

DB_PATH = resolve_sqlite_db_path_str()
PDF_STORAGE_DIR = Path("data/history_pdfs")

class JudgmentPipeline:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        ensure_history_schema(self.db_path)
        self.scraper = ORIApiScraper()
        self.processor = FinalJudgmentProcessor()
        PDF_STORAGE_DIR.mkdir(parents=True, exist_ok=True)

    def get_pending_judgments(self, limit: int = 10):
        """Get 3rd party auctions that haven't had their judgment processed."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            # We target Third Party sales with a PDF URL
            query = """
                SELECT auction_id, case_number, pdf_url, auction_date
                FROM history_auctions
                WHERE buyer_type = 'Third Party'
                AND pdf_url IS NOT NULL
                AND last_judgment_scan_at IS NULL
                LIMIT ?
            """
            return conn.execute(query, [limit]).fetchall()
        finally:
            conn.close()

    def _extract_instrument(self, pdf_url: str) -> str | None:
        """Extract instrument number from Clerk URL."""
        if "OBKey__1006_1=" in pdf_url:
            return pdf_url.split("OBKey__1006_1=")[1].split("&")[0]
        return None

    def update_judgment_data(self, auction_id: str, data: dict, amounts: dict):
        """Save extracted judgment data to database."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("""
                UPDATE history_auctions SET
                    pdf_judgment_amount = ?,
                    pdf_principal_amount = ?,
                    pdf_interest_amount = ?,
                    pdf_attorney_fees = ?,
                    pdf_court_costs = ?,
                    judgment_red_flags = ?,
                    judgment_data_json = ?,
                    last_judgment_scan_at = datetime('now')
                WHERE auction_id = ?
            """, [
                amounts.get('total_judgment_amount'),
                amounts.get('principal_amount'),
                amounts.get('interest_amount'),
                amounts.get('attorney_fees'),
                amounts.get('court_costs'),
                json.dumps(data.get('red_flags', [])),
                json.dumps(data),
                auction_id
            ])
            conn.commit()
        finally:
            conn.close()

    def mark_scanned(self, auction_id: str):
        """Mark as scanned even if failed, to avoid infinite retry."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                "UPDATE history_auctions SET last_judgment_scan_at = datetime('now') WHERE auction_id = ?",
                [auction_id],
            )
            conn.commit()
        finally:
            conn.close()

    async def process_batch(self, limit: int = 10):
        targets = self.get_pending_judgments(limit)
        if not targets:
            logger.info("No pending judgments to process.")
            return

        logger.info(f"Processing {len(targets)} judgments...")

        for row in targets:
            auction_id = row["auction_id"]
            case_num = row["case_number"]
            pdf_url = row["pdf_url"]
            auction_date = row["auction_date"]

            instrument = self._extract_instrument(pdf_url)
            if not instrument:
                logger.warning(f"Could not extract instrument from {pdf_url}")
                self.mark_scanned(auction_id)
                continue

            try:
                logger.info(f"Downloading PDF for instrument {instrument} (Case {case_num})...")
                # Create a minimal doc dict for the downloader
                doc = {"Instrument": instrument.strip() if isinstance(instrument, str) else instrument}
                pdf_path = self.scraper.download_pdf(doc, PDF_STORAGE_DIR)

                if not pdf_path or not pdf_path.exists():
                    logger.error(f"Failed to download PDF for {case_num}")
                    self.mark_scanned(auction_id)
                    continue

                logger.info(f"Processing PDF {pdf_path.name}...")
                result = self.processor.process_pdf(str(pdf_path), case_num)

                if result:
                    amounts = self.processor.extract_key_amounts(result)
                    self.update_judgment_data(auction_id, result, amounts)
                    logger.success(f"Successfully processed judgment for {case_num}")
                else:
                    logger.warning(f"No data extracted from PDF for {case_num}")
                    self.mark_scanned(auction_id)

            except Exception as e:
                logger.error(f"Error processing judgment for {case_num}: {e}")
                self.mark_scanned(auction_id)

if __name__ == "__main__":
    pipeline = JudgmentPipeline()
    asyncio.run(pipeline.process_batch(5))
