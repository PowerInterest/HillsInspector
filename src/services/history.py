"""
History Service - Manages historical auction data and flip analysis in PostgreSQL.
Consolidates logic from legacy src/history module.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

import re
from typing import Any, Optional, List, Dict

from bs4 import BeautifulSoup
from loguru import logger
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import (
    Column, Integer, String, Date, Numeric, 
    DateTime, Text, Float, func, text as sa_text,
)
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.orm import Session

from sunbiz.db import get_engine, resolve_pg_dsn
from sunbiz.models import Base

# =============================================================================
# Models
# =============================================================================

class HistoricalAuction(Base):
    """
    Consolidated table for historical auction results from HillsForeclosures.
    Links to PostgreSQL bulk data via folio/strap.
    """
    __tablename__ = "historical_auctions"

    id = Column(Integer, primary_key=True)
    listing_id = Column(String, unique=True, nullable=False)  # Hidden input 'listing_id'
    case_number = Column(String, index=True)                  # Hidden input 'case_number' (UNMASKED)
    auction_date = Column(Date, index=True)
    auction_status = Column(String)                           # 'Sold', 'Cancelled', etc.
    
    # Property Identifiers
    folio = Column(String, index=True)                        # 10-digit
    strap = Column(String, index=True)                        # Full strap
    property_address = Column(Text)
    
    # Financials
    winning_bid = Column(Numeric(18, 2))
    final_judgment_amount = Column(Numeric(18, 2))
    appraised_value = Column(Numeric(18, 2))
    previous_sale_price = Column(Numeric(18, 2))
    previous_sale_date = Column(Date)
    
    # Hidden/Rich Data
    latitude = Column(Float)
    longitude = Column(Float)
    photo_urls = Column(JSONB)                                # Array of absolute URLs
    
    # Specs
    bedrooms = Column(Numeric)
    bathrooms = Column(Numeric)
    sqft_total = Column(Integer)
    year_built = Column(Integer)
    
    # Metadata
    sold_to = Column(Text)                                    # Buyer name
    buyer_type = Column(String)                               # LLC, Individual, Bank, etc.
    html_path = Column(Text)                                  # Local path to original scrape
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

# =============================================================================
# Extraction Layer (Pydantic for validation)
# =============================================================================

class ExtractedHistoryRecord(BaseModel):
    listing_id: str
    case_number: Optional[str] = None
    auction_date: Optional[date] = None
    auction_status: Optional[str] = None
    folio: Optional[str] = None
    strap: Optional[str] = None
    property_address: Optional[str] = None
    winning_bid: Optional[float] = None
    final_judgment_amount: Optional[float] = None
    appraised_value: Optional[float] = None
    previous_sale_price: Optional[float] = None
    previous_sale_date: Optional[date] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    photo_urls: List[str] = Field(default_factory=list)
    bedrooms: Optional[float] = None
    bathrooms: Optional[float] = None
    sqft_total: Optional[int] = None
    year_built: Optional[int] = None
    sold_to: Optional[str] = None
    html_path: Optional[str] = None

    @field_validator("auction_date", "previous_sale_date", mode="before")
    @classmethod
    def parse_date(cls, v: Any) -> Optional[date]:
        if not v:
            return None
        if isinstance(v, date):
            return v
        # Handle formats like "August 27th, 2019" or "08/27/2019"
        v_clean = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", str(v))
        for fmt in ("%B %d, %Y", "%m/%d/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(v_clean, fmt).date()
            except ValueError:
                continue
        return None

# =============================================================================
# Service Class
# =============================================================================

class HistoricalScraper:
    """
    Scraper for historical auction results from HillsForeclosures.
    Logic moved from legacy src/history/scrape_history.py.
    """
    def __init__(
        self,
        service: HistoryService,
        max_concurrent: int = 1,
        headless: bool = True,
    ):
        self.service = service
        self.max_concurrent = max_concurrent
        self.headless = headless
        self.base_url = "https://hillsborough.realforeclose.com"
        
    async def run(self, start_date: date, end_date: date):
        """Scrape a range of dates and save to PostgreSQL."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context()
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            
            # Simple bypass splash
            await page.goto(f"{self.base_url}/index.cfm", wait_until="networkidle")
            
            current = start_date
            while current <= end_date:
                if current.weekday() < 5:
                    logger.info(f"Scraping history for {current}")
                    # placeholder
                current += timedelta(days=1)
            
            await browser.close()

class HistoryService:
    def __init__(self, dsn: Optional[str] = None):
        self.dsn = dsn or resolve_pg_dsn()
        self.engine = get_engine(self.dsn)
        self._ensure_schema()

    def _ensure_schema(self):
        """Create tables, indexes, and triggers in PostgreSQL."""
        logger.info("Ensuring historical_auctions schema in PostgreSQL...")
        # Casting to any to satisfy ty's strict sequence checking for SQLAlchemy internal types
        tables: Any = [HistoricalAuction.__table__]
        Base.metadata.create_all(self.engine, tables=tables)
        self._ensure_buyer_trigger()

    def _ensure_buyer_trigger(self):
        """Create (or replace) the trigger that auto-resolves placeholder buyer names.

        Logic:
          - CT/CD (Certificate of Title / Certificate of Deed): grantee IS the auction winner
            (Clerk issues certificate TO the winner; grantor is the old foreclosed owner)
          - WD/QC/DD/TR/FD (resale deeds): grantor IS the auction winner (they are selling)
        """
        ddl = """
            CREATE OR REPLACE FUNCTION resolve_buyer_name()
            RETURNS TRIGGER AS $$
            BEGIN
                IF lower(COALESCE(NEW.sold_to, '')) IN (
                    '3rd party bidder','third party bidder','3rd party','third party','unknown',''
                ) AND NEW.folio IS NOT NULL
                THEN
                    SELECT
                        CASE WHEN s.sale_type IN ('CT','CD') THEN s.grantee ELSE s.grantor END
                    INTO NEW.sold_to
                    FROM hcpa_allsales s
                    WHERE s.folio = NEW.folio
                      AND s.sale_date > NEW.auction_date
                      AND COALESCE(s.sale_amount, 0) > 0
                      AND s.sale_type IN ('CT','CD','WD','QC','TR','FD','DD')
                      AND COALESCE(
                          CASE WHEN s.sale_type IN ('CT','CD') THEN s.grantee ELSE s.grantor END,
                          ''
                      ) <> ''
                    ORDER BY s.sale_date ASC
                    LIMIT 1;

                    NEW.buyer_type := CASE
                        WHEN NEW.sold_to ~* 'BANK|MORTGAGE|LOAN|LENDING|FINANCIAL|PLAINTIFF|SERVICING|ASSOCIATION|TRUST'
                        THEN 'Plaintiff' ELSE 'Third Party' END;
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            DROP TRIGGER IF EXISTS trg_resolve_buyer ON historical_auctions;
            CREATE TRIGGER trg_resolve_buyer
            BEFORE INSERT OR UPDATE ON historical_auctions
            FOR EACH ROW EXECUTE FUNCTION resolve_buyer_name();
        """
        with self.engine.connect() as conn:
            conn.execute(sa_text(ddl))
            conn.commit()

    def parse_html_file(self, file_path: Path) -> Optional[ExtractedHistoryRecord]:
        """
        High-integrity parser for HillsForeclosures property pages.
        Extracts hidden fields and rich metadata.
        """
        try:
            html = file_path.read_text(encoding="utf-8", errors="ignore")
            soup = BeautifulSoup(html, "html.parser")
            
            # 1. Hidden Fields (Critical)
            listing_id_el = soup.find("input", {"id": "listing_id"})
            case_number_el = soup.find("input", {"id": "case_number"})
            
            if listing_id_el is None or not listing_id_el.get("value"):
                logger.warning(f"No listing_id found in {file_path}")
                return None
                
            lid = str(listing_id_el.get("value", ""))
            cnum = str(case_number_el.get("value", "")) if case_number_el else None

            data: dict[str, Any] = {
                "listing_id": lid,
                "case_number": cnum,
                "html_path": str(file_path)
            }

            # 2. Coordinates from L.marker
            coord_match = re.search(r"L\.marker\(\[([0-9.\-]+),\s*([0-9.\-]+)\]", html)
            if coord_match:
                data["latitude"] = float(coord_match.group(1))
                data["longitude"] = float(coord_match.group(2))

            # 3. Photo URLs
            photo_urls: List[str] = []
            for img in soup.select(".propertyImage, .propertyImageBox a"):
                url = None
                if img.name == "a":
                    url = img.get("href")
                elif "style" in img.attrs:
                    style_attr = str(img.get("style", ""))
                    match = re.search(r"url\('([^']+)'\)", style_attr)
                    if match:
                        url = match.group(1)
                
                if url and "/pictures/" in str(url):
                    final_url = str(url)
                    if not final_url.startswith("http"):
                        final_url = "https://www.hillsforeclosures.com" + final_url
                    if final_url not in photo_urls:
                        photo_urls.append(final_url)
            data["photo_urls"] = photo_urls

            # 4. Property Info & Auction Details
            text_content = soup.get_text(" ", strip=True)
            
            def find_labeled_val(label: str, text: str) -> Optional[str]:
                match = re.search(rf"{label}\s*:\s*([^:]+?)(?:\s*[A-Z][a-z]+|$)", text)
                return match.group(1).strip() if match else None

            h1 = soup.find("h1")
            data["property_address"] = h1.get_text(" ", strip=True).replace(" Foreclosure Information", "") if h1 else None
            
            data["auction_status"] = find_labeled_val("Auction Status", text_content)
            data["auction_date"] = find_labeled_val("Date Of Auction", text_content)
            data["sold_to"] = find_labeled_val("Winners Name", text_content)
            
            bid_match = re.search(r"Winning\s+Bid\s*:\s*\$([\d,]+)", text_content)
            if bid_match:
                data["winning_bid"] = float(bid_match.group(1).replace(",", ""))
                
            debt_match = re.search(r"Appraised\s*:\s*\$([\d,]+)", text_content)
            if debt_match:
                data["appraised_value"] = float(debt_match.group(1).replace(",", ""))

            spec_match = re.search(r"(\d+)\s*/\s*(\d+)\s*Bed\s*/\s*Bath", text_content)
            if spec_match:
                data["bedrooms"] = float(spec_match.group(1))
                data["bathrooms"] = float(spec_match.group(2))

            sqft_match = re.search(r"SQFT\s+Total\s*:\s*(\d+)", text_content)
            if sqft_match:
                data["sqft_total"] = int(sqft_match.group(1))

            year_match = re.search(r"Year\s+Built\s*:\s*(\d{4})", text_content)
            if year_match:
                data["year_built"] = int(year_match.group(1))

            return ExtractedHistoryRecord(**data)

        except Exception as e:
            logger.error(f"Failed to parse {file_path}: {e}")
            return None

    def upsert_records(self, session: Session, records: List[ExtractedHistoryRecord]):
        """Bulk upsert records using ON CONFLICT logic."""
        if not records:
            return

        for record in records:
            data = record.model_dump()
            stmt = insert(HistoricalAuction).values(data)
            stmt = stmt.on_conflict_do_update(
                index_elements=[HistoricalAuction.listing_id],
                set_={k: v for k, v in data.items() if k != "listing_id"}
            )
            session.execute(stmt)
        session.commit()

    def run_bulk_ingestion(self, directory: Path):
        """Walk a directory of HTML files and ingest all found records."""
        html_files = list(directory.glob("*.html"))
        logger.info(f"Starting bulk ingestion of {len(html_files)} files from {directory}")
        
        records: List[ExtractedHistoryRecord] = []
        count = 0
        
        with Session(self.engine) as session:
            for file_path in html_files:
                record = self.parse_html_file(file_path)
                if record:
                    records.append(record)
                
                if len(records) >= 100:
                    self.upsert_records(session, records)
                    count += len(records)
                    records = []
                    logger.info(f"Ingested {count}/{len(html_files)} files...")
            
            if records:
                self.upsert_records(session, records)
                count += len(records)
        
        logger.success(f"Bulk ingestion complete. Total records: {count}")

    def run_flip_analysis(self) -> List[Dict[str, Any]]:
        """
        PostgreSQL-native flip analysis using first valid post-auction sale.

        Valid sale rules:
        - sale_date > auction_date
        - sale_amount > 0
        - skip transfers at same price as winning bid (administrative)
        - skip nominal transfers below 10% of value reference
          (value reference: appraised_value, then winning_bid)
        """
        query = """
            SELECT
                a.listing_id,
                a.case_number,
                a.winning_bid,
                a.auction_date,
                fs.sale_date,
                fs.sale_amount,
                fs.sale_type,
                (fs.sale_amount - a.winning_bid) AS gross_profit,
                CASE
                    WHEN a.winning_bid > 0 THEN (fs.sale_amount / a.winning_bid)
                    ELSE NULL
                END AS roi,
                EXTRACT(DAY FROM (fs.sale_date::timestamp - a.auction_date::timestamp)) AS hold_time
            FROM historical_auctions a
            LEFT JOIN LATERAL (
                SELECT
                    s.sale_date,
                    s.sale_amount,
                    s.sale_type
                FROM hcpa_allsales s
                WHERE s.folio = a.folio
                  AND s.sale_date > a.auction_date
                  AND COALESCE(s.sale_amount, 0) > 0
                  AND (
                      a.winning_bid IS NULL
                      OR a.winning_bid <= 0
                      OR ABS(s.sale_amount - a.winning_bid) >= 1
                  )
                  AND s.sale_amount >= 0.10 * COALESCE(
                      NULLIF(a.appraised_value, 0),
                      NULLIF(a.winning_bid, 0),
                      s.sale_amount
                  )
                ORDER BY s.sale_date
                LIMIT 1
            ) fs ON TRUE
            WHERE fs.sale_date IS NOT NULL
              AND a.winning_bid > 0
            ORDER BY fs.sale_date DESC;
        """
        with self.engine.connect() as conn:
            results = conn.execute(sa_text(query)).fetchall()
            return [dict(r._asdict()) for r in results]

    def enrich_buyer_types(self):
        """Categorize buyers (Plaintiff vs Third Party) based on name patterns."""
        query = """
            UPDATE historical_auctions
            SET buyer_type = CASE
                WHEN sold_to ~* 'BANK|MORTGAGE|LOAN|LENDING|FINANCIAL|PLAINTIFF|SERVICING|ASSOCIATION|TRUST' THEN 'Plaintiff'
                WHEN sold_to IS NOT NULL AND sold_to != '' THEN 'Third Party'
                ELSE 'Unknown'
            END
            WHERE buyer_type IS NULL OR buyer_type = 'Unknown';
        """
        with self.engine.connect() as conn:
            conn.execute(sa_text(query))
            conn.commit()
            logger.info("Enriched buyer types in PostgreSQL.")

    def backfill_buyers_from_hcpa(self) -> int:
        """Resolve placeholder buyer names from hcpa_allsales.

        The auction website only records '3rd Party Bidder' — it never exposes
        who actually won.  The real buyer is identified as:
          - CT deed: grantee (Certificate of Title issued directly to auction winner)
          - WD/QC/DD/TR/CD deeds: grantor (auction winner is now selling the property)

        Updates sold_to and re-classifies buyer_type for any row whose
        current sold_to is a known placeholder.  Safe to run repeatedly.

        Returns the number of rows updated.
        """
        query = """
            UPDATE historical_auctions a
            SET sold_to = s.buyer_name,
                buyer_type = CASE
                    WHEN s.buyer_name ~* 'BANK|MORTGAGE|LOAN|LENDING|FINANCIAL|PLAINTIFF|SERVICING|ASSOCIATION|TRUST'
                    THEN 'Plaintiff' ELSE 'Third Party' END,
                updated_at = NOW()
            FROM (
                SELECT DISTINCT ON (ha.id) ha.id,
                    CASE WHEN s.sale_type IN ('CT','CD') THEN s.grantee ELSE s.grantor END AS buyer_name
                FROM historical_auctions ha
                JOIN hcpa_allsales s ON s.folio = ha.folio
                    AND s.sale_date > ha.auction_date
                    AND COALESCE(s.sale_amount, 0) > 0
                    AND s.sale_type IN ('CT','CD','WD','QC','TR','FD','DD')
                WHERE lower(COALESCE(ha.sold_to,'')) IN (
                    '3rd party bidder','third party bidder',
                    '3rd party','third party','unknown',''
                )
                  AND ha.folio IS NOT NULL
                  AND COALESCE(
                      CASE WHEN s.sale_type IN ('CT','CD') THEN s.grantee ELSE s.grantor END,
                      ''
                  ) <> ''
                ORDER BY ha.id, s.sale_date ASC
            ) s
            WHERE a.id = s.id
        """
        with self.engine.connect() as conn:
            result = conn.execute(sa_text(query))
            conn.commit()
            updated = result.rowcount
            logger.info(f"backfill_buyers_from_hcpa: updated {updated} rows")
            return updated

if __name__ == "__main__":
    service = HistoryService()
    test_dir = Path("data/temp/hills_benchmark_2y_full/rolling_2y/pages")
    if test_dir.exists():
        service.run_bulk_ingestion(test_dir)
