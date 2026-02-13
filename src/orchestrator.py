import asyncio
import json
import re
import contextlib
from datetime import date, datetime, timedelta
from typing import List, Optional, Dict, Any
from loguru import logger

# Models
from src.models.property import Property

# Scrapers & Services
from src.scrapers.tax_scraper import TaxScraper
from src.scrapers.permit_scraper import PermitScraper
from src.scrapers.market_scraper import MarketScraper
from src.scrapers.hcpa_gis_scraper import scrape_hcpa_property
from src.scrapers.sunbiz_scraper import SunbizScraper
from src.scrapers.ori_scraper import ORIScraper
from src.scrapers.fema_flood_scraper import FEMAFloodChecker, FEMARequestError
from src.services.ingestion_service import IngestionService
from src.services.lien_survival_analyzer import LienSurvivalAnalyzer
from src.services.homeharvest_service import HomeHarvestService

# Step 4v2 - Iterative Discovery
from src.services.step4v2 import IterativeDiscovery, ChainBuilder

# Database & Storage
from src.db.operations import PropertyDB
from src.db.writer import DatabaseWriter
from src.services.scraper_storage import ScraperStorage
from src.utils.legal_description import parse_legal_description, generate_search_permutations, combine_legal_fields
from src.utils.time import today_local

# Invalid folio values to skip - these are often scraped incorrectly from the auction site
INVALID_FOLIO_VALUES = {
    'property appraiser', 'n/a', 'none', '', 'unknown', 'pending',
    'see document', 'multiple', 'various', 'tbd', 'na'
}


def is_valid_folio(folio: str) -> bool:
    """
    Validate that a folio/parcel ID is a real parcel number, not garbage data.
    Returns False for empty, known invalid values, too short, or all-letter values.
    """
    if not folio:
        return False
    folio_clean = folio.strip().lower()
    if folio_clean in INVALID_FOLIO_VALUES:
        return False
    if len(folio_clean) < 6:
        return False
    return any(c.isdigit() for c in folio_clean)


def _display_status_summary(
    db: PropertyDB,
    start_date: date | None = None,
    end_date: date | None = None,
) -> None:
    """Display a summary of pipeline status for the given date range (or all)."""
    summary = db.get_status_summary(start_date, end_date)

    total = summary["total"]
    by_status = summary["by_status"]
    by_type = summary["by_type"]
    step_counts = summary["step_counts"]
    failures = summary["failures"]

    print()
    print("=" * 70)
    print("PIPELINE STATUS SUMMARY")
    print("=" * 70)
    if start_date and end_date:
        days = (end_date - start_date).days + 1
        print(f"Date Range: {start_date} to {end_date} ({days} days)")
    else:
        print("Scope: All auctions")
    print()

    if total == 0:
        print("No auctions tracked in this date range yet.")
        print("Pipeline will scrape new auctions during Step 1.")
        print("=" * 70)
        print()
        return

    # Auction type breakdown
    foreclosures = by_type.get("FORECLOSURE", 0)
    tax_deeds = by_type.get("TAX_DEED", 0)
    print(f"Total Auctions: {total}")
    if foreclosures or tax_deeds:
        print(f"  Foreclosures: {foreclosures}")
        print(f"  Tax Deeds:    {tax_deeds}")
    print()

    # Status breakdown with progress bar
    completed = by_status.get("completed", 0)
    processing = by_status.get("processing", 0)
    pending = by_status.get("pending", 0)
    failed = by_status.get("failed", 0)
    skipped = by_status.get("skipped", 0)

    def progress_bar(count: int, total: int, width: int = 25) -> str:
        if total == 0:
            return "[" + " " * width + "]"
        filled = int(width * count / total)
        return "[" + "#" * filled + "-" * (width - filled) + "]"

    print("By Status:")
    if total > 0:
        print(f"  Completed:   {completed:4d} ({100*completed//total:2d}%) {progress_bar(completed, total)}")
        print(f"  Processing:  {processing:4d} ({100*processing//total:2d}%) {progress_bar(processing, total)}")
        print(f"  Pending:     {pending:4d} ({100*pending//total:2d}%) {progress_bar(pending, total)}")
        print(f"  Failed:      {failed:4d} ({100*failed//total:2d}%) {progress_bar(failed, total)}")
        if skipped > 0:
            print(f"  Skipped:     {skipped:4d} ({100*skipped//total:2d}%) {progress_bar(skipped, total)}")
    print()

    # Step progress
    step_labels = {
        "step_auction_scraped": ("1  ", "Auction Scraped"),
        "step_pdf_downloaded": ("1  ", "PDF Downloaded"),
        "step_judgment_extracted": ("2  ", "Judgment Extracted"),
        "step_bulk_enriched": ("3  ", "Bulk Enriched"),
        "step_homeharvest_enriched": ("3.5", "HomeHarvest"),
        "step_hcpa_enriched": ("4  ", "HCPA Enriched"),
        "step_ori_ingested": ("5  ", "ORI Ingested"),
        "step_survival_analyzed": ("6  ", "Survival Analyzed"),
        "step_permits_checked": ("7  ", "Permits Checked"),
        "step_flood_checked": ("8  ", "Flood Checked"),
        "step_market_fetched": ("9  ", "Market Fetched"),
        "step_tax_checked": ("12 ", "Tax Checked"),
    }

    print("Step Progress:")
    for step_col, (step_num, label) in step_labels.items():
        count = step_counts.get(step_col, 0)
        pct = 100 * count // total if total > 0 else 0
        print(f"  Step {step_num} - {label:20s}: {count:4d}/{total} ({pct:2d}%)")
    print()

    # Recent failures
    if failures:
        print(f"Recent Failures (showing {len(failures)} of {failed}):")
        for f in failures[:5]:
            case = f["case_number"]
            step = f["error_step"] or "?"
            error = f["last_error"] or "Unknown error"
            retries = f["retry_count"]
            # Truncate error message
            if len(error) > 45:
                error = error[:42] + "..."
            print(f"  {case} @ Step {step}: {error} (retries: {retries})")
        print()

    print("=" * 70)
    print()


def show_status_summary(
    start_date: date | None = None,
    end_date: date | None = None,
) -> None:
    """Initialize/backfill status and print a summary for the date range."""
    if not start_date:
        start_date = today_local()
    if not end_date:
        end_date = start_date + timedelta(days=40)

    with PropertyDB() as db:
        db.ensure_status_table()
        db.initialize_status_from_auctions()
        db.backfill_status_steps(start_date, end_date)
        db.refresh_status_completion_for_range(start_date, end_date)
        _display_status_summary(db, start_date, end_date)


def verify_status(
    start_date: date | None = None,
    end_date: date | None = None,
) -> None:
    """Reconcile status table against files on disk for the date range."""
    from pathlib import Path

    if not start_date:
        start_date = today_local()
    if not end_date:
        end_date = start_date + timedelta(days=40)

    with PropertyDB() as db:
        db.ensure_status_table()
        db.initialize_status_from_auctions()
        db.backfill_status_steps(start_date, end_date)

        rows = db.execute_query(
            """
            SELECT case_number, parcel_id
            FROM status
            WHERE auction_date >= ? AND auction_date <= ?
              AND step_pdf_downloaded IS NULL
            """,
            (start_date, end_date),
        )

        foreclosure_dir = Path("data/Foreclosure")
        properties_dir = Path("data/properties")
        legacy_dir = Path("data/pdfs/final_judgments")
        updated = 0

        for row in rows:
            case_number = row.get("case_number")
            parcel_id = row.get("parcel_id") or ""
            if not case_number:
                continue

            candidates: list[Path] = []
            # Primary: data/Foreclosure/{case_number}/documents/ (where auction_scraper saves)
            sanitized_case = case_number.replace("/", "_").replace("\\", "_").replace(":", "_")
            candidates.append(foreclosure_dir / sanitized_case / "documents")
            # Secondary: data/properties/{folio}/documents/
            if parcel_id:
                sanitized = parcel_id.replace("/", "_").replace("\\", "_").replace(":", "_")
                candidates.append(properties_dir / sanitized / "documents")
            candidates.append(properties_dir / f"unknown_case_{case_number}" / "documents")

            has_pdf = False
            for doc_dir in candidates:
                if doc_dir.exists() and list(doc_dir.glob("final_judgment*.pdf")):
                    has_pdf = True
                    break

            if not has_pdf:
                legacy_path = legacy_dir / f"{case_number}_final_judgment.pdf"
                has_pdf = legacy_path.exists()

            if has_pdf:
                db.mark_status_step_complete(case_number, "step_pdf_downloaded", 1)
                updated += 1

        db.refresh_status_completion_for_range(start_date, end_date)
        _display_status_summary(db, start_date, end_date)

        logger.info(f"Verify complete: marked {updated} PDFs as downloaded.")


class PipelineOrchestrator:
    """
    Orchestrates the scraping and analysis pipeline.
    """
    
    def __init__(
        self,
        db_writer: DatabaseWriter,
        max_concurrent_properties: int = 15,
        db: Optional[PropertyDB] = None,
        storage: Optional[ScraperStorage] = None,
    ):
        self.db_writer = db_writer
        self.db = db or PropertyDB()  # Read-only access for status checks
        self.storage = storage or ScraperStorage(db_path=self.db.db_path, db=self.db)
        
        # Services
        self.tax_scraper = TaxScraper(storage=self.storage)
        self.permit_scraper = PermitScraper(headless=True, use_vision=True, storage=self.storage)
        self.market_scraper = MarketScraper(headless=True, storage=self.storage)
        self.sunbiz_scraper = SunbizScraper(headless=True, storage=self.storage)
        self.fema_checker = FEMAFloodChecker(storage=self.storage)
        
        # Heavy Services (Injected with db_writer for serialization)
        self.ingestion_service = IngestionService(
            db_writer=self.db_writer,
            db=self.db,
            storage=self.storage,
        )
        self.survival_analyzer = LienSurvivalAnalyzer()
        self.homeharvest_service = HomeHarvestService(db=self.db)
        
        # Concurrency Control
        self.property_semaphore = asyncio.Semaphore(max_concurrent_properties)
        self.market_semaphore = asyncio.Semaphore(3)
        self.tax_semaphore = asyncio.Semaphore(5)
        self.permit_semaphore = asyncio.Semaphore(5)
        self.hcpa_semaphore = asyncio.Semaphore(5)
        self.sunbiz_semaphore = asyncio.Semaphore(5)
        self.fema_semaphore = asyncio.Semaphore(10)
        self.homeharvest_semaphore = asyncio.Semaphore(1)

    def folio_has_chain_of_title(self, folio: str) -> bool:
        """Check if folio has chain of title data."""
        return self.db.folio_has_chain_of_title(folio)

    def get_chain_of_title(self, folio: str) -> Dict[str, Any]:
        """Get chain of title records."""
        return self.db.get_chain_of_title(folio)

    def get_encumbrances_by_folio(self, folio: str) -> List[dict]:
        """Get encumbrances for folio."""
        return self.db.get_encumbrances_by_folio(folio)

    def encumbrance_exists(self, folio: str, book: str, page: str) -> bool:
        """Check if encumbrance exists by book/page."""
        return self.db.encumbrance_exists(folio, book, page)

    def _gather_and_analyze_survival(self, prop: Property) -> Dict[str, Any]:
        """
        Synchronous worker method for Survival Analysis.
        Reads DB, runs logic, returns updates to be applied.
        """
        folio = prop.parcel_id
        case_number = prop.case_number
        
        # 1. Gather Data
        auction = self.db.get_auction_by_case(case_number)
        if not auction:
            return {
                "error": "Missing auction record for survival analysis",
                "folio": folio,
                "case_number": case_number,
            }
        
        encs_rows = self.get_encumbrances_by_folio(folio)
        chain = self.get_chain_of_title(folio)
        
        current_owner_acq_date = None
        if chain and chain.get("ownership_timeline"):
             acq = chain["ownership_timeline"][-1].get("acquisition_date")
             if acq:
                 with contextlib.suppress(ValueError, TypeError):
                     current_owner_acq_date = datetime.strptime(str(acq), "%Y-%m-%d").date()

        # Prepare encumbrances list
        encumbrances = []
        enc_id_map = {}
        for row in encs_rows:
            rec_date = None
            if row["recording_date"]:
                with contextlib.suppress(ValueError):
                    rec_date = datetime.strptime(str(row["recording_date"]), "%Y-%m-%d").date()
            
            enc = {
                "id": row["id"],
                "encumbrance_type": row["encumbrance_type"],
                "recording_date": rec_date,
                "creditor": row.get("creditor"),
                "debtor": row.get("debtor"),
                "amount": row["amount"],
                "instrument": row.get("instrument"),
                "book": row.get("book"),
                "page": row.get("page"),
                "is_satisfied": row.get("is_satisfied", False),
            }
            encumbrances.append(enc)
            instrument = row.get("instrument")
            book = row.get("book")
            page = row.get("page")
            rec_date_key = rec_date.isoformat() if rec_date else None
            row_id = row["id"]
            if instrument:
                key = f"INST:{instrument}"
            elif book and page:
                key = f"BKPG:{book}/{page}"
            else:
                # Include row ID in fallback key to avoid collisions when
                # multiple encumbrances share the same date+type (e.g., HOA liens)
                key = f"DTYPE:{rec_date_key}_{row['encumbrance_type']}_{row_id}"
            enc_id_map[key] = row_id

        # 2. Identify Foreclosing Mortgage
        judgment_data = {}
        raw_judgment = auction.get("extracted_judgment_data")
        if isinstance(raw_judgment, dict):
            judgment_data = raw_judgment
        elif isinstance(raw_judgment, str) and raw_judgment.strip():
            try:
                judgment_data = json.loads(raw_judgment)
            except (json.JSONDecodeError, TypeError) as e:
                raw_preview = raw_judgment[:200].replace("\n", " ").replace("\r", " ")
                error_msg = (
                    f"Invalid extracted_judgment_data JSON for survival analysis "
                    f"(case={case_number}, folio={folio}): {e}"
                )
                logger.error(f"{error_msg}; raw_preview={raw_preview!r}")
                return {"error": error_msg, "folio": folio, "case_number": case_number}

        foreclosed_mtg = judgment_data.get("foreclosed_mortgage", {})
        mtg_book = foreclosed_mtg.get("recording_book")
        mtg_page = foreclosed_mtg.get("recording_page")
        
        foreclosing_refs = {
            "instrument": foreclosed_mtg.get("instrument_number"),
            "book": mtg_book,
            "page": mtg_page
        }
        
        new_encumbrances = [] # List of dicts to insert
        
        if mtg_book and mtg_page and not self.encumbrance_exists(folio, mtg_book, mtg_page):
            # Check if this mortgage is already in our list (maybe scraped but missed book match?)
            # Or perform lookup
            mtg_instrument = foreclosed_mtg.get("instrument_number")
            mtg_record_date = foreclosed_mtg.get("recording_date")
            
            if not mtg_instrument:
                try:
                    # Sync lookup via ORIScraper (instantiated here/stateless)
                    ori_scraper = ORIScraper() # Logic should be in scraper class
                    ori_results = ori_scraper.search_by_book_page_sync(mtg_book, mtg_page)
                    if ori_results:
                        for ori_doc in ori_results:
                            doc_type = ori_doc.get("ORI - Doc Type", "")
                            if "MTG" in doc_type or "MORTGAGE" in doc_type.upper():
                                mtg_instrument = ori_doc.get("Instrument #")
                                if not mtg_record_date:
                                    dt = ori_doc.get("Recording Date Time", "").split()[0]
                                    mtg_record_date = dt if dt else None
                                break
                        if not mtg_instrument and ori_results:
                             mtg_instrument = ori_results[0].get("Instrument #")
                except Exception as e:
                    logger.warning(f"Failed to lookup mortgage by book/page: {e}")
            
            if mtg_instrument:
                foreclosing_refs["instrument"] = mtg_instrument
            
            # Create the encumbrance entry for insertion
            mtg_amount = judgment_data.get("principal_amount") or foreclosed_mtg.get("original_amount")
            mtg_creditor = auction.get("plaintiff")
            
            new_enc = {
                "folio": folio,
                "encumbrance_type": "mortgage",
                "creditor": mtg_creditor,
                "amount": mtg_amount,
                "recording_date": mtg_record_date,
                "book": mtg_book,
                "page": mtg_page,
                "instrument": mtg_instrument,
                "survival_status": "FORECLOSING",
                "is_inferred": True # Since we created it from judgment inference
            }
            new_encumbrances.append(new_enc)
            
            # Add to local list for analyzer
            rec_date_parsed = None
            if mtg_record_date:
                with contextlib.suppress(ValueError, TypeError):
                    rec_date_parsed = datetime.strptime(str(mtg_record_date), "%Y-%m-%d").date()

            encumbrances.append({
                 "encumbrance_type": "mortgage",
                 "creditor": mtg_creditor,
                 "amount": mtg_amount,
                 "recording_date": rec_date_parsed,
                 "book": mtg_book,
                 "page": mtg_page,
                 "instrument": mtg_instrument,
            })

        if not any(foreclosing_refs.values()):
            foreclosing_refs = None

        # 3. Analyze
        lis_pendens_date = None
        lp_str = judgment_data.get("lis_pendens_date")
        if lp_str:
            with contextlib.suppress(ValueError):
                lis_pendens_date = datetime.strptime(lp_str, "%Y-%m-%d").date()

        # Extract defendant names list from judgment data for "Joined" check (matches old pipeline)
        def_names = []
        defs = judgment_data.get("defendants")
        if isinstance(defs, list):
            for d in defs:
                name = d.get("name") if isinstance(d, dict) else str(d) if d else None
                if name:
                    def_names.append(name)
        elif isinstance(defs, dict):
            name = defs.get("name")
            if name:
                def_names = [name]
        elif isinstance(defs, str):
            def_names = [defs]

        # Fallback to single string if list missing (matches old pipeline)
        defendant = judgment_data.get("defendant")
        if not def_names and defendant:
            def_names = [defendant]

        survival_result = self.survival_analyzer.analyze(
            encumbrances=encumbrances,
            foreclosure_type=auction.get("foreclosure_type") or judgment_data.get("foreclosure_type"),
            lis_pendens_date=lis_pendens_date,
            current_owner_acquisition_date=current_owner_acq_date,
            plaintiff=auction.get("plaintiff"),
            original_mortgage_amount=auction.get("original_mortgage_amount"),
            foreclosing_refs=foreclosing_refs,
            defendants=def_names or None
        )
        
        # 4. Map Results to Updates
        updates = []
        results_by_status = survival_result.get("results", {})
        
        status_mapping = {
            "survived": "SURVIVED",
            "extinguished": "EXTINGUISHED",
            "expired": "EXPIRED",
            "satisfied": "SATISFIED",
            "historical": "HISTORICAL",
            "foreclosing": "FORECLOSING",
        }
        
        for category, status_val in status_mapping.items():
            for enc in results_by_status.get(category, []):
                enc_id = enc.get("encumbrance_id")
                if enc_id:
                    upd = {"encumbrance_id": enc_id, "status": status_val}
                    if enc.get("is_joined") is not None:
                        upd["is_joined"] = enc.get("is_joined")
                    if enc.get("is_inferred"):
                        upd["is_inferred"] = True
                    updates.append(upd)
                    continue

                instrument = enc.get("instrument")
                book = enc.get("book")
                page = enc.get("page")
                rec_date_key = enc.get("recording_date")
                # Get the original DB ID if preserved through analysis
                enc_orig_id = enc.get("id")
                if instrument:
                    key = f"INST:{instrument}"
                elif book and page:
                    key = f"BKPG:{book}/{page}"
                elif enc_orig_id:
                    # Use original ID in fallback key (matches map building)
                    key = f"DTYPE:{rec_date_key}_{enc.get('encumbrance_type') or enc.get('type')}_{enc_orig_id}"
                else:
                    # Fallback for new encumbrances without DB ID
                    key = f"DTYPE:{rec_date_key}_{enc.get('encumbrance_type') or enc.get('type')}"
                # If checking against new_encumbrances, they don't have DB IDs yet.
                # But they are "FORECLOSING" status already.
                # Only update EXISTING DB records.
                db_id = enc_id_map.get(key)
                if db_id:
                    upd = {"encumbrance_id": db_id, "status": status_val}
                    if enc.get("is_joined") is not None:
                        upd["is_joined"] = enc.get("is_joined")
                    if enc.get("is_inferred"):
                        upd["is_inferred"] = True
                    updates.append(upd)

        # Include summary for logging (matches old pipeline)
        summary = survival_result.get("summary", {})

        return {
            "new_encumbrances": new_encumbrances,
            "updates": updates,
            "summary": summary,
            "folio": folio,
            "case_number": case_number
        }

    def _gather_and_analyze_survival_v2(
        self, prop: Property, auction: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Phase 3: Survival Analysis v2.
        Uses property_master_v2.db and modular SurvivalService.

        Args:
            prop: Property object
            auction: Pre-fetched auction data (to avoid cross-thread DB access)
        """
        from src.services.lien_survival.survival_service import SurvivalService
        from src.services.step4v2.chain_builder import ChainBuilder

        folio = prop.parcel_id
        case_number = prop.case_number

        if not auction:
            return {"error": "No auction record for case"}

        try:
            conn = self.db.connect()

            # 1. Fetch data from SQLite DB
            builder = ChainBuilder(conn)
            periods = builder.get_chain(folio)
            encumbrances = builder.get_encumbrances(folio)

            # Map to dicts for service
            period_dicts = []
            for p in periods:
                d = p.__dict__.copy()
                if d.get('acquisition_date'):
                    d['acquisition_date'] = d['acquisition_date']
                period_dicts.append(d)

            enc_dicts = []
            for e in encumbrances:
                d = e.__dict__.copy()
                enc_dicts.append(d)
            
            judgment_data = {}
            raw_judgment = auction.get("extracted_judgment_data")
            if isinstance(raw_judgment, dict):
                judgment_data = raw_judgment
            elif isinstance(raw_judgment, str) and raw_judgment.strip():
                try:
                    judgment_data = json.loads(raw_judgment)
                except (json.JSONDecodeError, TypeError) as e:
                    raw_preview = raw_judgment[:200].replace("\n", " ").replace("\r", " ")
                    error_msg = (
                        f"Invalid extracted_judgment_data JSON for survival analysis "
                        f"(case={case_number}, folio={folio}): {e}"
                    )
                    logger.error(f"{error_msg}; raw_preview={raw_preview!r}")
                    return {"error": error_msg, "folio": folio, "case_number": case_number}
            
            # Ensure critical context is present
            if not judgment_data.get('plaintiff'):
                judgment_data['plaintiff'] = auction.get('plaintiff')
            if not judgment_data.get('defendants'):
                def_name = auction.get('defendant')
                if def_name:
                    judgment_data['defendants'] = [def_name]
            
            # Current period: Usually the one with disposition_date IS NULL or latest acquisition
            current_period_id = None
            if periods:
                latest = sorted(periods, key=lambda p: p.acquisition_date or date.min, reverse=True)[0]
                current_period_id = latest.id
            
            # 3. Analyze
            service = SurvivalService(folio)
            analysis = service.analyze(enc_dicts, judgment_data, period_dicts, current_period_id)
            
            # 4. Collect updates to return (writes happen via DatabaseWriter in caller)
            flat_results = []
            for cat in analysis['results'].values():
                flat_results.extend(cat)

            survival_updates = []
            for enc in flat_results:
                if enc.get('id'):
                    survival_updates.append({
                        'id': enc['id'],
                        'survival_status': enc['survival_status'],
                        'survival_reason': enc.get('survival_reason'),
                    })

            # Return updates summary — writes happen via DatabaseWriter in caller
            return {
                "updates": [],
                "survival_updates": survival_updates,
                "summary": {
                    "survived_count": len(analysis['results']['survived']),
                    "extinguished_count": len(analysis['results']['extinguished']),
                    "historical_count": len(analysis['results']['historical']),
                    "foreclosing_count": len(analysis['results']['foreclosing']),
                },
                "uncertainty_flags": analysis.get("uncertainty_flags", []),
                "full_summary": analysis.get("summary", ""),
                "folio": folio,
                "case_number": case_number
            }
            
        except Exception as e:
            logger.exception(f"Survival analysis v2 failed for {folio}: {e}")
            return {"error": str(e)}

    async def process_auctions(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        include_failed: bool = False,
        max_retries: int = 3,
        skip_tax_deeds: bool = False,
    ):
        """
        Main entry point. Enriches auctions, optionally filtered by date range.
        When dates are None, processes all incomplete auctions.
        """
        if start_date and end_date:
            logger.info(f"Starting orchestration for {start_date} to {end_date}")
        else:
            logger.info("Starting orchestration for all incomplete auctions")

        auctions = self.db.get_auctions_for_processing(
            start_date,
            end_date,
            include_failed=include_failed,
            max_retries=max_retries,
            skip_tax_deeds=skip_tax_deeds,
        )
        self._log_resume_stats(
            start_date,
            end_date,
            auctions,
            include_failed=include_failed,
            max_retries=max_retries,
            skip_tax_deeds=skip_tax_deeds,
        )
        logger.info(f"Found {len(auctions)} auctions to process")

        await self._process_batch(auctions)

        logger.success("Orchestration complete")

    def _log_resume_stats(
        self,
        start_date: date | None,
        end_date: date | None,
        auctions: List[dict],
        include_failed: bool,
        max_retries: int,
        skip_tax_deeds: bool,
    ) -> None:
        summary = self.db.get_status_summary(start_date, end_date)
        total = summary["total"]
        by_status = summary["by_status"]
        completed = by_status.get("completed", 0)
        processing = by_status.get("processing", 0)
        pending = by_status.get("pending", 0)
        failed = by_status.get("failed", 0)
        skipped = by_status.get("skipped", 0)
        retryable_failed = 0
        if failed:
            retryable_failed = len(self.db.get_failed_cases(start_date, end_date, max_retries))
        date_label = f"{start_date} to {end_date}" if start_date and end_date else "all dates"
        logger.info(
            "Resume stats "
            f"({date_label}): total={total} "
            f"pending={pending} processing={processing} completed={completed} "
            f"skipped={skipped} failed={failed} retryable_failed={retryable_failed} "
            f"to_process={len(auctions)} include_failed={include_failed} "
            f"skip_tax_deeds={skip_tax_deeds}"
        )

    async def _process_batch(self, properties: List[dict]):
        """
        Process a list of properties concurrently.
        """
        async with asyncio.TaskGroup() as tg:
            for auction_dict in properties:
                tg.create_task(self._enrich_property_safe(auction_dict))

    async def _enrich_property_safe(self, auction_dict: dict):
        """Wrapper to handle semaphore and errors for a single property."""
        async with self.property_semaphore:
            try:
                await self._enrich_property(auction_dict)
            except Exception as e:
                logger.exception(f"Failed to enrich property {auction_dict.get('parcel_id')}: {e}")

    async def _enrich_property(self, auction_dict: dict):
        """
        Fan-Out: Run all scrapers for a single property concurrently.
        Executes in dependent phases.
        """
        parcel_id = auction_dict.get('parcel_id')
        case_number = auction_dict.get('case_number')

        if not case_number:
            logger.info(f"Skipping enrichment: No case_number for parcel {parcel_id}")
            return

        status_state = self.db.get_status_state(case_number)
        if status_state in {"completed", "skipped"}:
            logger.info(f"Skipping {case_number}: status={status_state}")
            return

        # Determine address early
        address = auction_dict.get('address') or auction_dict.get('location_address') or auction_dict.get('property_address') or "Unknown"

        if not parcel_id:
            logger.warning(
                f"Skipping enrichment: No parcel_id for case {case_number}. "
                "Run Step 2.5 parcel resolution (requires judgment extraction and bulk_parcels) "
                "to restore downstream enrichment coverage."
            )
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_status_skipped, "args": [case_number, "No parcel_id"]},
            )
            return

        # Instantiate Property Object EARLY for consistency (Code Health Improvement)
        prop = Property(
            case_number=case_number,
            parcel_id=parcel_id,
            address=address,
            owner_name=auction_dict.get('owner_name'),
            legal_description=auction_dict.get('legal_description'),
            plaintiff=auction_dict.get('plaintiff'),
            defendant=auction_dict.get('defendant')
        )

        # Pre-hydrate from bulk parcel data (parcels + bulk_parcels tables)
        # This fills in address/owner that the auction scraper may not have,
        # preventing the "Skipping enrichment: No address" bail below.
        if is_valid_folio(parcel_id):
            bulk_data = self.db.get_parcel_by_folio(parcel_id)
            if bulk_data:
                if prop.address == "Unknown" and bulk_data.get("address"):
                    prop.address = bulk_data["address"]
                    address = prop.address
                    logger.info(f"Hydrated address from bulk data for {parcel_id}: {address}")
                if not prop.owner_name and bulk_data.get("owner_name"):
                    prop.owner_name = bulk_data["owner_name"]

        # Check for invalid folios (mobile homes, "Property Appraiser", etc.)
        if not is_valid_folio(parcel_id):
            if prop.plaintiff or prop.defendant:
                logger.info(f"Invalid folio '{parcel_id}' for case {case_number}, trying party-based ORI search")
                try:
                    # Run party-based ingestion in executor with skip_db_writes=True
                    # Then queue the writes through db_writer to avoid write-write conflicts
                    loop = asyncio.get_running_loop()
                    result = await loop.run_in_executor(
                        None,
                        lambda: self.ingestion_service.ingest_property_by_party(
                            prop, prop.plaintiff, prop.defendant, skip_db_writes=True
                        )
                    )
                    # Queue the DB writes if we got results
                    if result and result.get('documents'):
                        for doc in result['documents']:
                            await self.db_writer.execute_with_result(
                                self.db.save_document, result['property_id'], doc
                            )
                        if result.get('chain_data'):
                            await self.db_writer.execute_with_result(
                                self.db.save_chain_of_title, result['property_id'], result['chain_data']
                            )
                    await self.db_writer.enqueue(
                        "generic_call",
                        {
                            "func": self.db.mark_status_step_complete,
                            "args": [case_number, "step_ori_ingested", 5],
                        },
                    )
                    await self.db_writer.enqueue(
                        "generic_call",
                        {
                            "func": self.db.mark_status_skipped,
                            "args": [
                                case_number,
                                "Invalid parcel_id; processed via party-based ORI only",
                            ],
                        },
                    )
                    # Mark step complete
                    await self.db_writer.enqueue("generic_call", {
                        "func": self.db.mark_step_complete,
                        "args": [case_number, "needs_ori_ingestion"]
                    })
                except Exception as e:
                    logger.error(f"Party-based ingestion failed for {case_number}: {e}")
                    await self.db_writer.enqueue(
                        "generic_call",
                        {
                            "func": self.db.mark_status_failed,
                            "args": [case_number, str(e)[:200], 5],
                        },
                    )
            else:
                logger.info(f"Invalid folio '{parcel_id}' and no party data. Skipping ORI.")
                await self.db_writer.enqueue("generic_call", {
                    "func": self.db.mark_step_complete,
                    "args": [case_number, "needs_ori_ingestion"]
                })
                await self.db_writer.enqueue("generic_call", {
                    "func": self.db.mark_status_step_complete,
                    "args": [case_number, "step_ori_ingested", 5]
                })
                await self.db_writer.enqueue(
                    "generic_call",
                    {
                        "func": self.db.mark_status_skipped,
                        "args": [case_number, "Invalid parcel_id; no party data"],
                    },
                )
            return

        if prop.address == "Unknown":
            logger.info(f"Skipping enrichment for {parcel_id}: No address")
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_status_skipped, "args": [case_number, "No address available"]},
            )
            return

        logger.info(f"Enriching {parcel_id} ({prop.address})")

        # PHASE 1: Independent Parallel Scrapers (Data Gathering)
        # These don't depend on each other and can run immediately
        # Use gather(return_exceptions=True) instead of TaskGroup so that one
        # failing scraper (e.g. tax timeout) doesn't cancel all other tasks.
        logger.info(f"Phase 1: Starting parallel gather for {parcel_id}")
        phase1_results = await asyncio.gather(
            self._run_tax_scraper(case_number, parcel_id, prop.address),
            self._run_market_scraper(case_number, parcel_id, prop.address),
            self._run_homeharvest(prop),
            self._run_fema_checker(case_number, parcel_id, prop.address),
            self._run_sunbiz_scraper(parcel_id, prop.owner_name or ""),
            self._run_hcpa_gis(case_number, parcel_id),
            return_exceptions=True,
        )
        for i, result in enumerate(phase1_results):
            if isinstance(result, Exception):
                task_names = ["tax", "market", "homeharvest", "fema", "sunbiz", "hcpa"]
                logger.warning(f"Phase 1 task {task_names[i]} failed for {parcel_id}: {result}")

        # PHASE 2: ORI Ingestion (Depends on Legal Description from HCPA/Bulk)
        # HCPA GIS (Phase 1) might have updated legal description in DB
        logger.info(f"Phase 2: Starting ORI Ingestion for {parcel_id}")

        # Skip logic: folio has chain AND same case number (matches old pipeline)
        last_case = self.db.get_last_analyzed_case(parcel_id)
        if self.folio_has_chain_of_title(parcel_id) and last_case == case_number:
            logger.info(f"Skipping ORI for {parcel_id} - already analyzed for {case_number}")
            await self.db_writer.enqueue("generic_call", {
                "func": self.db.mark_step_complete,
                "args": [case_number, "needs_ori_ingestion"]
            })
            await self.db_writer.enqueue("generic_call", {
                "func": self.db.mark_status_step_complete,
                "args": [case_number, "step_ori_ingested", 5]
            })
        else:
            # Get legal description with FULL FALLBACK CHAIN (matches old pipeline):
            # 1. HCPA legal_description from parcels table
            # 2. Judgment legal_description from parcels table
            # 3. Bulk raw_legal1-4 from bulk_parcels table
            primary_legal = None
            legal_source = None

            try:
                conn = self.db.connect()
                parcel_data = conn.execute(
                    """SELECT legal_description, judgment_legal_description
                       FROM parcels WHERE folio = ?""", [parcel_id]
                ).fetchone()
                if parcel_data:
                    hcpa_legal = parcel_data[0]
                    judgment_legal = parcel_data[1]
                    if hcpa_legal:
                        primary_legal = hcpa_legal
                        legal_source = "HCPA"
                    elif judgment_legal:
                        primary_legal = judgment_legal
                        legal_source = "JUDGMENT"
            except Exception as e:
                logger.debug(f"Failed to load legal descriptions for {parcel_id}: {e}")

            # Fallback to bulk_parcels raw_legal fields (matches old pipeline)
            if not primary_legal:
                try:
                    conn = self.db.connect()
                    bp = conn.execute(
                        """SELECT raw_legal1, raw_legal2, raw_legal3, raw_legal4
                           FROM bulk_parcels WHERE strap = ?""", [parcel_id]
                    ).fetchone()
                    if bp:
                        primary_legal = combine_legal_fields(bp[0], bp[1], bp[2], bp[3])
                        if primary_legal:
                            legal_source = "BULK_RAW_LEGAL"
                except Exception as e:
                    logger.debug(f"Failed to load bulk_parcels raw_legal for {parcel_id}: {e}")

            if not primary_legal:
                # No legal description from HCPA/judgment/bulk - try party-based search as fallback
                # This can find Lis Pendens which contains the legal description
                plaintiff = prop.plaintiff or auction_dict.get('plaintiff')
                defendant = prop.defendant or auction_dict.get('defendant')

                if plaintiff or defendant:
                    logger.info(f"No legal description for {parcel_id}, trying party-based ORI search")
                    try:
                        # Run party-based ingestion in executor with skip_db_writes=True
                        # Then queue the writes through db_writer to avoid write-write conflicts
                        loop = asyncio.get_running_loop()
                        result = await loop.run_in_executor(
                            None,
                            lambda: self.ingestion_service.ingest_property_by_party(
                                prop, plaintiff, defendant, skip_db_writes=True
                            )
                        )
                        # Queue the DB writes if we got results
                        if result and result.get('documents'):
                            for doc in result['documents']:
                                await self.db_writer.execute_with_result(
                                    self.db.save_document, result['property_id'], doc
                                )
                            if result.get('chain_data'):
                                await self.db_writer.execute_with_result(
                                    self.db.save_chain_of_title, result['property_id'], result['chain_data']
                                )
                        await self.db_writer.enqueue("generic_call", {
                            "func": self.db.mark_status_step_complete,
                            "args": [case_number, "step_ori_ingested", 5]
                        })
                        await self.db_writer.enqueue(
                            "generic_call",
                            {
                                "func": self.db.mark_ori_party_fallback_used,
                                "args": [
                                    case_number,
                                    "Party-based ORI fallback used (no legal description)",
                                ],
                            },
                        )
                        await self.db_writer.enqueue("generic_call", {
                            "func": self.db.mark_step_complete,
                            "args": [case_number, "needs_ori_ingestion"]
                        })
                        # Don't mark HCPA as failed - party search succeeded, HCPA didn't fail.
                        # The case can continue with other enrichment steps since folio is valid.
                        logger.success(f"Party-based ORI search completed for {case_number} (no legal desc fallback)")
                    except Exception as e:
                        logger.error(f"Party-based ORI search failed for {case_number}: {e}")
                        await self.db_writer.enqueue("generic_call", {
                            "func": self.db.mark_status_failed,
                            "args": [case_number, f"Party search failed: {str(e)[:150]}", 5]
                        })
                        logger.error(
                            f"Aborting further enrichment for {case_number}: "
                            "party-based ORI fallback failed and legal description is still missing."
                        )
                        return
                else:
                    # No legal description AND no party data - mark for manual review
                    logger.warning(f"No usable legal description for {parcel_id} (case {case_number}) and no party data, marking for manual review")
                    await self.db_writer.enqueue("generic_call", {
                        "func": self.db.mark_hcpa_scrape_failed,
                        "args": [case_number, "No usable legal description (HCPA/judgment/bulk) and no party data"],
                    })
                    # Mark complete so we don't loop forever
                    await self.db_writer.enqueue("generic_call", {
                        "func": self.db.mark_step_complete,
                        "args": [case_number, "needs_ori_ingestion"]
                    })
                    await self.db_writer.enqueue("generic_call", {
                        "func": self.db.mark_status_step_complete,
                        "args": [case_number, "step_ori_ingested", 5]
                    })
            else:
                # Build search terms (matches old pipeline logic exactly)
                prop.legal_description = primary_legal
                parsed = parse_legal_description(primary_legal)
                terms: list[str | tuple] = list(generate_search_permutations(parsed, primary_legal))

                # Add filter info for post-search filtering
                lot_filter = parsed.lots or ([parsed.lot] if parsed.lot else None)
                if lot_filter or parsed.block:
                    filter_info = {
                        "lot": lot_filter,
                        "block": parsed.block,
                        "subdivision": parsed.subdivision,
                        "require_all_lots": isinstance(lot_filter, list) and len(lot_filter) > 1,
                    }
                    terms.append(("__filter__", filter_info))

                # Metes-and-bounds fallback: use 60-char prefix if no search terms (matches old pipeline)
                if not terms or (len(terms) == 1 and isinstance(terms[0], tuple)):
                    # Strip leading section numbers (e.g., "1\tBEG..." or "1 BEG...")
                    clean_legal = re.sub(r'^\d+[\t\s]+', '', primary_legal)
                    prefix = clean_legal.upper().strip()[:60]
                    if prefix:
                        terms.insert(0, f"{prefix}*")

                prop.legal_search_terms = terms
                logger.info(f"  Legal ({legal_source}): {primary_legal}")
                logger.info(f"  Search terms: {terms}")

                await self._run_ori_ingestion(case_number, prop)

                # Mark step complete after successful ingestion (matches old pipeline)
                await self.db_writer.enqueue("generic_call", {
                    "func": self.db.mark_step_complete,
                    "args": [case_number, "needs_ori_ingestion"]
                })

        # Barrier: flush all pending db_writer writes so Phase 3 reads see Phase 2 data.
        # Without this, step_ori_ingested and document/chain writes from Phase 2 may
        # still be in the db_writer queue when survival reads the DB directly.
        await self.db_writer.execute_with_result(self.db.checkpoint)

        # PHASE 3: Dependent Parallel Analysis (Needs ORI Data)
        # Permits needs NOCs (from ORI)
        # Survival needs Encumbrances (from ORI)
        logger.info(f"Phase 3: Starting Analysis for {parcel_id}")
        phase3_results = await asyncio.gather(
            self._run_permit_scraper(case_number, parcel_id, address),
            self._run_survival_analysis(case_number, prop),
            return_exceptions=True,
        )
        for i, result in enumerate(phase3_results):
            if isinstance(result, Exception):
                task_names = ["permits", "survival"]
                logger.warning(f"Phase 3 task {task_names[i]} failed for {parcel_id}: {result}")

    # -------------------------------------------------------------------------
    # Individual Execution Wrappers
    # -------------------------------------------------------------------------

    async def _run_tax_scraper(self, case_number: str, parcel_id: str, address: str):
        if self.db.is_status_step_complete(case_number, "step_tax_checked"):
            return
        if self.db.folio_has_tax_data(parcel_id):
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_status_step_complete, "args": [case_number, "step_tax_checked", 12]},
            )
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_step_complete, "args": [case_number, "needs_tax_check"]},
            )
            return
        if not address or address.lower() in ("unknown", "n/a", "none", ""):
            logger.info(f"Tax: missing address for {case_number}, marking step complete (permanent)")
            await self.db_writer.enqueue(
                "generic_call",
                {
                    "func": self.storage.record_scrape,
                    "kwargs": {
                        "property_id": parcel_id,
                        "scraper": "tax_collector",
                        "success": False,
                        "error": "Missing address for tax search",
                        "source_url": getattr(self.tax_scraper, "BASE_URL", None),
                    },
                },
            )
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_status_step_complete, "args": [case_number, "step_tax_checked", 12]},
            )
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_step_complete, "args": [case_number, "needs_tax_check"]},
            )
            return
        async with self.tax_semaphore:
            try:
                tax_status = await self.tax_scraper.scrape_tax_status(parcel_id, address)
                if tax_status is None:
                    await self.db_writer.enqueue(
                        "generic_call",
                        {
                            "func": self.storage.record_scrape,
                            "kwargs": {
                                "property_id": parcel_id,
                                "scraper": "tax_collector",
                                "success": False,
                                "error": "Tax scraper returned no data",
                                "source_url": getattr(self.tax_scraper, "BASE_URL", None),
                            },
                        },
                    )
                    await self.db_writer.enqueue(
                        "generic_call",
                        {"func": self.db.mark_status_step_failed, "args": [case_number, "Tax scraper returned no data", 12]},
                    )
                    return

                # Check if we got actual scraped data (not just input echoed back)
                # NOTE: Exclude situs - it's set from input address, not scraped data
                has_data = any([
                    tax_status.account_number,
                    tax_status.owner,
                    tax_status.paid_in_full,
                    tax_status.amount_due and tax_status.amount_due > 0,
                    tax_status.last_payment,
                    tax_status.certificates,
                ])
                if not has_data:
                    await self.db_writer.enqueue(
                        "generic_call",
                        {
                            "func": self.storage.record_scrape,
                            "kwargs": {
                                "property_id": parcel_id,
                                "scraper": "tax_collector",
                                "screenshot_path": tax_status.screenshot_path,
                                "success": False,
                                "error": "Tax search returned no results",
                                "vision_data": {
                                    "account_number": tax_status.account_number,
                                    "owner": tax_status.owner,
                                    "situs": tax_status.situs,
                                    "amount_due": tax_status.amount_due,
                                    "paid_in_full": tax_status.paid_in_full,
                                    "last_payment": tax_status.last_payment,
                                    "certificates": [
                                        cert.model_dump() for cert in tax_status.certificates
                                    ],
                                },
                                "prompt_version": "v1",
                                "source_url": getattr(self.tax_scraper, "BASE_URL", None),
                            },
                        },
                    )
                    await self.db_writer.enqueue(
                        "generic_call",
                        {"func": self.db.mark_status_step_failed, "args": [case_number, "Tax search returned no results", 12]},
                    )
                    return
                # Derive status strings
                status_str = "PAID" if tax_status.paid_in_full else ("DELINQUENT" if tax_status.amount_due > 0 else "UNKNOWN")
                has_warrant = len(tax_status.certificates) > 0
                
                await self.db_writer.enqueue("update_tax_status", {"folio": parcel_id, "tax_status": status_str, "tax_warrant": has_warrant})
                await self.db_writer.enqueue(
                    "generic_call",
                    {
                        "func": self.storage.record_scrape,
                        "kwargs": {
                            "property_id": parcel_id,
                            "scraper": "tax_collector",
                            "screenshot_path": tax_status.screenshot_path,
                            "success": True,
                            "vision_data": {
                                "account_number": tax_status.account_number,
                                "owner": tax_status.owner,
                                "situs": tax_status.situs,
                                "amount_due": tax_status.amount_due,
                                "paid_in_full": tax_status.paid_in_full,
                                "last_payment": tax_status.last_payment,
                                "certificates": [
                                    cert.model_dump() for cert in tax_status.certificates
                                ],
                                "tax_status": status_str,
                                "tax_warrant": has_warrant,
                            },
                            "prompt_version": "v1",
                            "source_url": getattr(self.tax_scraper, "BASE_URL", None),
                        },
                    },
                )
                await self.db_writer.enqueue(
                    "generic_call",
                    {"func": self.db.mark_status_step_complete, "args": [case_number, "step_tax_checked", 12]},
                )
                await self.db_writer.enqueue(
                    "generic_call",
                    {"func": self.db.mark_step_complete, "args": [case_number, "needs_tax_check"]},
                )
            except Exception as e:
                logger.warning(f"Tax scraper failed: {e}")
                await self.db_writer.enqueue(
                    "generic_call",
                    {
                        "func": self.storage.record_scrape,
                        "kwargs": {
                            "property_id": parcel_id,
                            "scraper": "tax_collector",
                            "success": False,
                            "error": str(e)[:200],
                            "source_url": getattr(self.tax_scraper, "BASE_URL", None),
                        },
                    },
                )
                await self.db_writer.enqueue(
                    "generic_call",
                    {"func": self.db.mark_status_step_failed, "args": [case_number, str(e)[:200], 12]},
                )

    async def _run_market_scraper(self, case_number: str, parcel_id: str, address: str):
        # Check if we already have recent market data
        if self.db.is_status_step_complete(case_number, "step_market_fetched"):
            return
        if self.db.folio_has_market_data(parcel_id):
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_status_step_complete, "args": [case_number, "step_market_fetched", 9]},
            )
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_step_complete, "args": [case_number, "needs_market_data"]},
            )
            return

        # Validate address - can't search market data without a real address
        if not address or address.lower() in ("unknown", "n/a", "none", ""):
            logger.warning(f"Skipping market scrape for {case_number}: invalid address '{address}'")
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_status_step_complete, "args": [case_number, "step_market_fetched", 9]},
            )
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_step_complete, "args": [case_number, "needs_market_data"]},
            )
            return

        async with self.market_semaphore:
            try:
                # Parse address components
                street = address
                city = "Tampa"
                state = "FL"
                zip_code = ""

                parts = address.split(",")
                if len(parts) >= 3:
                    street = parts[0].strip()
                    city = parts[1].strip()
                    state_zip = parts[2].strip().split()
                    if len(state_zip) > 0:
                        state = state_zip[0]
                    if len(state_zip) > 1:
                        zip_code = state_zip[1]
                elif len(parts) == 2:
                    # Handle "123 Main St, Tampa" format
                    street = parts[0].strip()
                    city_state_zip = parts[1].strip()
                    tokens = city_state_zip.split()
                    if len(tokens) >= 2 and tokens[-1].isdigit():
                        zip_code = tokens[-1]
                        if len(tokens) >= 3 and len(tokens[-2]) == 2:
                            state = tokens[-2]
                            city = " ".join(tokens[:-2])
                        else:
                            city = " ".join(tokens[:-1])
                    elif len(tokens) >= 2 and len(tokens[-1]) == 2:
                        state = tokens[-1]
                        city = " ".join(tokens[:-1])
                    else:
                        city = city_state_zip

                # get_listing_details now tries both Zillow and Realtor
                listing = await self.market_scraper.get_listing_details(
                    address=street, city=city, state=state, zip_code=zip_code, property_id=parcel_id
                )

                has_value = False
                if listing:
                    has_value = any([
                        listing.price is not None,
                        listing.status and listing.status != "Unknown",
                        bool(listing.estimates.get("Zillow")),
                        bool(listing.estimates.get("Rent Zestimate")),
                        bool(listing.estimates.get("Rent Estimate")),
                        listing.hoa_monthly is not None,
                        listing.days_on_market is not None,
                        bool(listing.price_history),
                    ])

                if listing and has_value:
                    market_payload = {
                        "listing_status": listing.status,
                        "list_price": listing.price,
                        "zestimate": listing.estimates.get("Zillow"),
                        "rent_estimate": (
                            listing.estimates.get("Rent Zestimate")
                            or listing.estimates.get("Rent Estimate")
                        ),
                        "hoa_monthly": listing.hoa_monthly,
                        "days_on_market": listing.days_on_market,
                        "price_history": listing.price_history,
                        "description": listing.description,
                    }
                    # Save consolidated market data
                    await self.db_writer.enqueue("save_market_data", {
                        "folio": parcel_id,
                        "source": "Consolidated",
                        "data": market_payload,
                        "screenshot_path": getattr(listing, "screenshot_path", None)
                    })
                    logger.success(f"Consolidated market data saved for {parcel_id}")
                    # Mark step complete - we got useful data
                    await self.db_writer.enqueue(
                        "generic_call",
                        {"func": self.db.mark_status_step_complete, "args": [case_number, "step_market_fetched", 9]},
                    )
                    await self.db_writer.enqueue(
                        "generic_call",
                        {"func": self.db.mark_step_complete, "args": [case_number, "needs_market_data"]},
                    )
                else:
                    logger.warning(f"Market scrape returned no useful data for {parcel_id}")
                    await self.db_writer.enqueue(
                        "generic_call",
                        {"func": self.db.mark_status_step_failed, "args": [case_number, "No useful market data returned", 9]},
                    )
            except Exception as e:
                logger.warning(f"Market scraper failed for {parcel_id}: {e}")
                await self.db_writer.enqueue(
                    "generic_call",
                    {"func": self.db.mark_status_step_failed, "args": [case_number, str(e)[:200], 9]},
                )

    async def _run_homeharvest(self, prop: Property):
        """Phase 1: Run HomeHarvest Enrichment."""
        if self.db.is_status_step_complete(prop.case_number, "step_homeharvest_enriched"):
            return
        if self.db.folio_has_homeharvest_data(prop.parcel_id):
            await self.db_writer.enqueue(
                "generic_call",
                {
                    "func": self.db.mark_status_step_complete,
                    "args": [prop.case_number, "step_homeharvest_enriched"],
                },
            )
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_step_complete, "args": [prop.case_number, "needs_homeharvest_enrichment"]},
            )
            return

        async with self.homeharvest_semaphore:
            logger.info(f"Running HomeHarvest for {prop.address}")
            loop = asyncio.get_running_loop()
            try:
                data, status = await loop.run_in_executor(
                    None,
                    lambda: self.homeharvest_service.fetch_record_data(
                        prop.parcel_id,
                        prop.address,
                        proxy=self.homeharvest_service.proxy,
                        is_first=False,  # Prevent auto-upgrade subprocess during pipeline
                    ),
                )
                if data:
                    await self.db_writer.enqueue(
                        "generic_call",
                        {"func": self.homeharvest_service.insert_record_data, "args": [data]},
                    )
                    logger.success(f"HomeHarvest data saved for {prop.parcel_id}")
                    await self.db_writer.enqueue(
                        "generic_call",
                        {
                            "func": self.db.mark_status_step_complete,
                            "args": [prop.case_number, "step_homeharvest_enriched"],
                        },
                    )
                    await self.db_writer.enqueue(
                        "generic_call",
                        {
                            "func": self.db.mark_step_complete,
                            "args": [prop.case_number, "needs_homeharvest_enrichment"],
                        },
                    )
                elif status in {"no_data"}:
                    await self.db_writer.enqueue(
                        "generic_call",
                        {
                            "func": self.db.mark_status_step_complete,
                            "args": [prop.case_number, "step_homeharvest_enriched"],
                        },
                    )
                elif status in {"blocked", "error"}:
                    await self.db_writer.enqueue(
                        "generic_call",
                        {"func": self.db.mark_status_step_failed, "args": [prop.case_number, f"HomeHarvest {status}", 3]},
                    )
            except SystemExit as e:
                # HomeHarvest auto-upgrade spawns a background subprocess and raises SystemExit.
                # That subprocess races with the pipeline for DB locks — treat as transient failure.
                logger.warning(f"HomeHarvest triggered upgrade/subprocess for {prop.parcel_id}, treating as transient")
                await self.db_writer.enqueue(
                    "generic_call",
                    {"func": self.db.mark_status_step_failed, "args": [prop.case_number, f"HomeHarvest upgrade: {e}", 3]},
                )
            except Exception as e:
                logger.error(f"HomeHarvest failed for {prop.parcel_id}: {e}")
                await self.db_writer.enqueue(
                    "generic_call",
                    {"func": self.db.mark_status_step_failed, "args": [prop.case_number, str(e)[:200], 3]},
                )

    async def _run_fema_checker(self, case_number: str, parcel_id: str, address: str):
        # Check cache via storage
        if self.db.is_status_step_complete(case_number, "step_flood_checked"):
            return
        if self.db.folio_has_flood_data(parcel_id):
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_status_step_complete, "args": [case_number, "step_flood_checked", 8]},
            )
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_step_complete, "args": [case_number, "needs_flood_check"]},
            )
            return
        async with self.fema_semaphore:
            try:
                # Fetch property to get coords (returns dict, not object)
                prop = self.db.get_property(parcel_id)
                if not prop:
                    logger.info(f"FEMA: no parcel data for {case_number}, marking step complete (permanent)")
                    await self.db_writer.enqueue(
                        "generic_call",
                        {"func": self.db.mark_status_step_complete, "args": [case_number, "step_flood_checked", 8]},
                    )
                    await self.db_writer.enqueue(
                        "generic_call",
                        {"func": self.db.mark_step_complete, "args": [case_number, "needs_flood_check"]},
                    )
                    return

                lat = prop.get("latitude") if isinstance(prop, dict) else getattr(prop, "latitude", None)
                lon = prop.get("longitude") if isinstance(prop, dict) else getattr(prop, "longitude", None)

                if lat is None or lon is None:
                    logger.info(f"FEMA: missing coordinates for {case_number}, marking step complete (permanent)")
                    await self.db_writer.enqueue(
                        "generic_call",
                        {"func": self.db.mark_status_step_complete, "args": [case_number, "step_flood_checked", 8]},
                    )
                    await self.db_writer.enqueue(
                        "generic_call",
                        {"func": self.db.mark_step_complete, "args": [case_number, "needs_flood_check"]},
                    )
                    return

                try:
                    lat_val = float(lat)
                    lon_val = float(lon)
                except (TypeError, ValueError):
                    logger.info(f"FEMA: invalid coordinates for {case_number}, marking step complete (permanent)")
                    await self.db_writer.enqueue(
                        "generic_call",
                        {"func": self.db.mark_status_step_complete, "args": [case_number, "step_flood_checked", 8]},
                    )
                    await self.db_writer.enqueue(
                        "generic_call",
                        {"func": self.db.mark_step_complete, "args": [case_number, "needs_flood_check"]},
                    )
                    return

                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(
                    None,
                    lambda: self.fema_checker.get_flood_zone_for_property(
                        parcel_id, lat_val, lon_val
                    ),
                )
                if not result:
                    await self.db_writer.enqueue(
                        "generic_call",
                        {"func": self.db.mark_status_step_failed, "args": [case_number, "FEMA query returned no result", 8]},
                    )
                    return

                await self.db_writer.enqueue("save_flood_data", {
                    "folio": parcel_id,
                    "flood_zone": result.flood_zone,
                    "flood_risk": result.risk_level,
                    "insurance_required": result.insurance_required
                })
                await self.db_writer.enqueue(
                    "generic_call",
                    {"func": self.db.mark_status_step_complete, "args": [case_number, "step_flood_checked", 8]},
                )
                await self.db_writer.enqueue(
                    "generic_call",
                    {"func": self.db.mark_step_complete, "args": [case_number, "needs_flood_check"]},
                )
            except FEMARequestError:
                logger.warning("FEMA unavailable; skipping flood check for {}", parcel_id)
                await self.db_writer.enqueue(
                    "generic_call",
                    {"func": self.db.mark_status_step_complete, "args": [case_number, "step_flood_checked", 8]},
                )
                await self.db_writer.enqueue(
                    "generic_call",
                    {"func": self.db.mark_step_complete, "args": [case_number, "needs_flood_check"]},
                )
            except Exception as e:
                logger.warning(f"FEMA failed: {e}")
                await self.db_writer.enqueue(
                    "generic_call",
                    {"func": self.db.mark_status_step_failed, "args": [case_number, str(e)[:200], 8]},
                )

    async def _run_sunbiz_scraper(self, parcel_id: str, owner_name: str):
        if not owner_name:
            logger.info(
                f"Skipping Sunbiz search for {parcel_id}: owner_name is missing"
            )
            return
        # Sunbiz checks owner name
        async with self.sunbiz_semaphore:
            try:
                await self.sunbiz_scraper.search_for_property(parcel_id, owner_name)
            except Exception as e:
                logger.warning(f"Sunbiz failed: {e}")

    async def _run_hcpa_gis(self, case_number: str, parcel_id: str):
        if self.db.is_status_step_complete(case_number, "step_hcpa_enriched"):
            return
        # NOTE: Removed folio_has_sales_history shortcut - it could skip even when
        # legal_description is missing. Rely on step_hcpa_enriched status instead.

        # Validate parcel_id - can't search HCPA without a valid parcel ID
        if not parcel_id or parcel_id.lower() in ("unknown", "n/a", "none", "", "property appraiser", "multiple parcel"):
            logger.warning(f"Skipping HCPA GIS for {case_number}: invalid parcel_id '{parcel_id}'")
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_status_step_complete, "args": [case_number, "step_hcpa_enriched", 4]},
            )
            return

        async with self.hcpa_semaphore:
            try:
                hcpa_storage = ScraperStorage()
                result = await scrape_hcpa_property(
                    parcel_id=parcel_id,
                    storage=hcpa_storage,
                    storage_key=case_number
                )

                # Check errors - transient (HCPA site may be down)
                if result.get("error"):
                    error_msg = result.get("error", "Unknown HCPA error")[:200]
                    await self.db_writer.enqueue(
                        "generic_call",
                        {"func": self.db.mark_status_step_failed, "args": [case_number, error_msg, 4]},
                    )
                    return

                # Save all HCPA data to parcels table (creates row if needed)
                await self.db_writer.enqueue(
                    "generic_call",
                    {
                        "func": self.db.save_hcpa_to_parcel,
                        "args": [parcel_id, result],
                    },
                )

                # Save Sales History via Writer
                has_sales = bool(result.get("sales_history"))
                has_legal = bool(result.get("legal_description"))

                if has_sales:
                    await self.db_writer.enqueue(
                        "generic_call",
                        {
                            "func": self.db.save_sales_history_from_hcpa,
                            "args": [parcel_id, result["sales_history"]],
                        },
                    )

                # Save new legal description
                if has_legal:
                    await self.db_writer.enqueue(
                        "generic_call",
                        {
                            "func": self.db.update_legal_description,
                            "args": [parcel_id, result["legal_description"]],
                        },
                    )

                # Only mark complete if we got useful data
                if has_sales or has_legal:
                    await self.db_writer.enqueue(
                        "generic_call",
                        {
                            "func": self.db.mark_step_complete_by_folio,
                            "args": [parcel_id, "needs_hcpa_enrichment"],
                        },
                    )
                    await self.db_writer.enqueue(
                        "generic_call",
                        {"func": self.db.mark_status_step_complete, "args": [case_number, "step_hcpa_enriched", 4]},
                    )
                else:
                    # No sales_history/legal_description from HCPA.
                    # If bulk data already provided owner+address, mark as completed
                    # so the property can proceed through the rest of the pipeline.
                    bulk_data = self.db.get_parcel_by_folio(parcel_id)
                    has_bulk_basics = (
                        bulk_data
                        and bulk_data.get("owner_name")
                        and bulk_data.get("address")
                    )
                    if has_bulk_basics:
                        logger.info(
                            f"HCPA returned no data for {parcel_id} but bulk data has owner+address, marking complete"
                        )
                        await self.db_writer.enqueue(
                            "generic_call",
                            {
                                "func": self.db.mark_step_complete_by_folio,
                                "args": [parcel_id, "needs_hcpa_enrichment"],
                            },
                        )
                        await self.db_writer.enqueue(
                            "generic_call",
                            {"func": self.db.mark_status_step_complete, "args": [case_number, "step_hcpa_enriched", 4]},
                        )
                    else:
                        logger.warning(f"HCPA GIS returned no sales_history or legal_description for {parcel_id}")
                        await self.db_writer.enqueue(
                            "generic_call",
                            {"func": self.db.mark_status_step_failed, "args": [case_number, "No sales_history or legal_description extracted", 4]},
                        )

            except Exception as e:
                logger.warning(f"HCPA GIS failed: {e}")
                await self.db_writer.enqueue(
                    "generic_call",
                    {"func": self.db.mark_status_step_failed, "args": [case_number, str(e)[:200], 4]},
                )

    async def _run_ori_ingestion(self, case_number: str, prop: Property):
        """Run ORI ingestion - dispatches to v1 or v2 based on config."""
        if self.db.is_status_step_complete(case_number, "step_ori_ingested"):
            return

        await self._run_ori_ingestion_v2(case_number, prop)

    async def _run_ori_ingestion_v1(self, case_number: str, prop: Property):
        """Run ORI ingestion using v1 (IngestionService)."""
        # IngestionService manages its own internal semaphores/concurrency via db_writer
        try:
            await self.ingestion_service.ingest_property_async(prop)
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_status_step_complete, "args": [case_number, "step_ori_ingested", 5]},
            )
        except Exception as e:
            logger.error(f"ORI Ingestion v1 failed: {e}")
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_status_failed, "args": [case_number, str(e)[:200], 5]},
            )

    async def _run_ori_ingestion_v2(self, case_number: str, prop: Property):
        """
        Run ORI ingestion using Step 4v2 (IterativeDiscovery).

        Uses the SQLite database and iterative discovery algorithm.
        """

        folio = prop.parcel_id
        if not folio:
            logger.info(f"ORI: no folio for {case_number}, marking step complete (permanent)")
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_status_step_complete, "args": [case_number, "step_ori_ingested", 5]},
            )
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_step_complete, "args": [case_number, "needs_ori_ingestion"]},
            )
            return

        try:
            conn = self.db.connect()

            # Get auction data
            auction = self.db.get_auction_by_case(case_number)
            if not auction:
                logger.error(f"No auction record for {case_number}")
                return

            # Get HCPA data
            hcpa_data = None
            parcel_row = conn.execute(
                "SELECT * FROM parcels WHERE folio = ? LIMIT 1", [folio]
            ).fetchone()
            if parcel_row:
                hcpa_data = dict(parcel_row)

            # Get bulk parcel data
            bulk_parcel = None
            try:
                bulk_row = conn.execute(
                    "SELECT * FROM bulk_parcels WHERE folio = ? OR strap = ? LIMIT 1", [folio, folio]
                ).fetchone()
                if bulk_row:
                    bulk_parcel = dict(bulk_row)
            except Exception as e:
                logger.debug(f"bulk_parcels query failed for {folio}: {e}")

            # Get final judgment data
            final_judgment = auction.get("extracted_judgment_data") or {}
            if isinstance(final_judgment, str):
                import json
                try:
                    final_judgment = json.loads(final_judgment)
                except json.JSONDecodeError as e:
                    logger.warning(f"Corrupt judgment JSON for {folio}: {e}")
                    final_judgment = {}

            # Run iterative discovery
            discovery = IterativeDiscovery(conn)
            result = discovery.run(
                folio=folio,
                auction=auction,
                hcpa_data=hcpa_data,
                final_judgment=final_judgment,
                bulk_parcel=bulk_parcel,
            )

            # Build chain of title from discovered documents
            chain_builder = ChainBuilder(conn)
            chain_result = chain_builder.build(folio)

            logger.info(
                f"Step 4v2 complete for {folio}: {result.documents_found} docs, "
                f"{chain_result.total_years:.1f} years chain, {len(chain_result.encumbrances)} encumbrances"
            )

            # Mark step complete
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_status_step_complete, "args": [case_number, "step_ori_ingested", 5]},
            )

        except Exception as e:
            logger.error(f"ORI Ingestion v2 failed for {case_number}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_status_failed, "args": [case_number, str(e)[:200], 5]},
            )

    async def _run_permit_scraper(self, case_number: str, parcel_id: str, address: str):
        if self.db.is_status_step_complete(case_number, "step_permits_checked"):
            return
        if self.db.folio_has_permits(parcel_id):
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_status_step_complete, "args": [case_number, "step_permits_checked", 7]},
            )
            return

        # Validate address - can't search permits without a real address
        if not address or address.strip().lower() in ("unknown", "n/a", "none", ""):
            logger.warning(f"Skipping permit check for {case_number}: invalid address '{address}'")
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_status_step_complete, "args": [case_number, "step_permits_checked", 7]},
            )
            return

        async with self.permit_semaphore:
            try:
                permits = await self.permit_scraper.get_permits_for_property(
                    case_number, address, "Tampa"
                )
                if permits:
                    await self.db_writer.enqueue(
                        "save_permits", {"folio": parcel_id, "permits": permits}
                    )
                await self.db_writer.enqueue(
                    "generic_call",
                    {"func": self.db.mark_status_step_complete, "args": [case_number, "step_permits_checked", 7]},
                )
                await self.db_writer.enqueue(
                    "generic_call",
                    {"func": self.db.mark_step_complete, "args": [case_number, "needs_permit_check"]},
                )
            except Exception as e:
                logger.error(f"Permit scraper failed: {e}")
                await self.db_writer.enqueue(
                    "generic_call",
                    {"func": self.db.mark_status_step_failed, "args": [case_number, str(e)[:200], 7]},
                )

    async def _run_survival_analysis(self, case_number: str, prop: Property):
        try:
            # Skip logic: folio has survival AND same case number (matches old pipeline)
            last_case = self.db.get_last_analyzed_case(prop.parcel_id)
            has_survival = self.db.folio_has_survival_analysis(prop.parcel_id)
            has_pending = self.db.folio_has_unanalyzed_encumbrances(prop.parcel_id)
            if has_survival and not has_pending and last_case == prop.case_number:
                logger.info(
                    f"Skipping survival for {prop.parcel_id} - already analyzed for {prop.case_number}"
                )
                await self.db_writer.enqueue(
                    "generic_call",
                    {
                        "func": self.db.mark_step_complete,
                        "args": [prop.case_number, "needs_lien_survival"],
                    },
                )
                await self.db_writer.enqueue(
                    "generic_call",
                    {
                        "func": self.db.mark_status_step_complete,
                        "args": [case_number, "step_survival_analyzed", 6],
                    },
                )
                return

            # Preflight: ORI must have completed for this case before survival runs.
            # Without ORI data, survival has nothing to analyze.
            ori_done = self.db.is_status_step_complete(case_number, "step_ori_ingested")
            if not ori_done:
                logger.info(
                    f"Skipping survival for {prop.parcel_id}: ORI not complete for case {case_number}"
                )
                return  # Don't mark step — deferred until ORI completes

            # Logic is heavy and synchronous (DB reads, potential ORI lookup). Run in executor.
            loop = asyncio.get_running_loop()

            # Pre-fetch auction data in main thread to avoid cross-thread DB access
            auction = self.db.get_auction_by_case(case_number)
            result = await loop.run_in_executor(
                None, self._gather_and_analyze_survival_v2, prop, auction
            )

            if not result:
                logger.warning(
                    f"Survival analysis produced no result for {prop.parcel_id} ({case_number})"
                )
                await self.db_writer.enqueue(
                    "generic_call",
                    {
                        "func": self.db.mark_status_failed,
                        "args": [case_number, "Survival analysis produced no result", 6],
                    },
                )
                return
            if result.get("error"):
                await self.db_writer.enqueue(
                    "generic_call",
                    {
                        "func": self.db.mark_status_failed,
                        "args": [case_number, result["error"], 6],
                    },
                )
                return

            updates = result.get("updates", [])
            for update in updates:
                await self.db_writer.enqueue(
                    "generic_call",
                    {
                        "func": self.db.update_encumbrance_survival,
                        "kwargs": update,
                    },
                )

            # Process survival status updates from v2 analysis (returned, not written in-thread)
            survival_updates = result.get("survival_updates", [])
            if not survival_updates:
                logger.warning(
                    f"Survival analysis for {prop.parcel_id} ({case_number}) returned 0 updates "
                    f"(flags={result.get('uncertainty_flags', [])}, summary={result.get('full_summary', '')})"
                )
            for upd in survival_updates:
                await self.db_writer.enqueue(
                    "generic_call",
                    {
                        "func": self.db.update_encumbrance_survival,
                        "kwargs": {
                            "encumbrance_id": upd["id"],
                            "status": upd["survival_status"],
                            "survival_reason": upd.get("survival_reason"),
                        },
                    },
                )

            new_encs = result.get("new_encumbrances", [])
            for enc in new_encs:
                await self.db_writer.enqueue(
                    "generic_call",
                    {
                        "func": self.db.insert_encumbrance,
                        "kwargs": enc,
                    },
                )

            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_as_analyzed, "args": [prop.case_number]},
            )
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.set_last_analyzed_case, "args": [prop.parcel_id, prop.case_number]},
            )

            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_step_complete, "args": [prop.case_number, "needs_lien_survival"]},
            )
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_status_step_complete, "args": [case_number, "step_survival_analyzed", 6]},
            )

            summary = result.get("summary", {})
            logger.info(
                f"  Survival for {prop.parcel_id}: "
                f"Survived: {summary.get('survived_count', 0)}, "
                f"Extinguished: {summary.get('extinguished_count', 0)}, "
                f"Historical: {summary.get('historical_count', 0)}, "
                f"Foreclosing: {summary.get('foreclosing_count', 0)}"
            )

        except Exception as e:
            logger.exception(f"Survival analysis failed for {prop.parcel_id}: {e}")
            await self.db_writer.enqueue(
                "generic_call",
                {"func": self.db.mark_status_failed, "args": [case_number, str(e)[:200], 6]},
            )


async def run_full_update(
    start_date: date | None = None,
    end_date: date | None = None,
    start_step: int = 1,
    geocode_missing_parcels: bool = True,
    geocode_limit: int | None = 25,
    skip_tax_deeds: bool = False,
    auction_limit: int | None = None,
    retry_failed: bool = False,
    max_retries: int = 3,
) -> None:
    """Run the full pipeline with a single orchestrator entrypoint."""
    from pathlib import Path

    from src.ingest.bulk_parcel_ingest import enrich_auctions_from_bulk
    from src.scrapers.auction_scraper import AuctionScraper
    from src.scrapers.tax_deed_scraper import TaxDeedScraper
    from src.services.final_judgment_processor import FinalJudgmentProcessor
    from src.services.vision_service import VisionService

    logger.info("Running FULL UPDATE pipeline (Orchestrator)...")

    # Health check vision endpoints at startup - only use responsive servers
    logger.info("Checking vision server availability...")
    healthy_endpoints = VisionService.health_check_endpoints(timeout=5)
    if not healthy_endpoints:
        logger.warning("No vision servers available - PDF analysis will be skipped")

    # Date window only gates Step 1 (scraping new auctions).
    # Steps 2+ process ALL incomplete auctions regardless of date.
    scrape_start = start_date if start_date else today_local()
    scrape_end = end_date if end_date else scrape_start + timedelta(days=40)

    db = PropertyDB()
    storage = ScraperStorage(db_path=db.db_path, db=db)

    # Initialize status table and backfill from existing data (all auctions)
    db.ensure_status_table()
    db.initialize_status_from_auctions()
    db.backfill_status_steps()
    db.refresh_status_completion_for_range()

    # Display pipeline status summary (all auctions)
    _display_status_summary(db)

    # =========================================================================
    # STEP 1 & 1.5: Scrape Auctions
    # =========================================================================
    if start_step <= 1:
        logger.info("=" * 60)
        logger.info("STEP 1: SCRAPING FORECLOSURE AUCTIONS")
        logger.info("=" * 60)

        try:
            foreclosure_scraper = AuctionScraper(
                storage=storage,
                process_final_judgments=False,
            )
            current = scrape_start
            while current <= scrape_end:
                if current.weekday() < 5:  # Skip weekends
                    count = db.get_auction_count_by_date(current)
                    if count == 0 and db.was_auction_scraped(current, "foreclosure"):
                        logger.info(f"Skipping {current}: previously scraped with 0 auctions")
                    elif count == 0:
                        logger.info(f"Scraping foreclosures for {current}...")
                        props = await foreclosure_scraper.scrape_date(
                            current,
                            fast_fail=True,
                            max_properties=auction_limit if auction_limit and auction_limit > 0 else None,
                        )
                        # Scraper writes to Inbox (Parquet) automatically.
                        # We only track the scrape event here.
                        db.record_auction_scrape(current, "foreclosure", len(props))
                        logger.success(f"Scraped {len(props)} auctions for {current}")
                    else:
                        logger.info(f"Skipping {current}: {count} auctions already in DB")
                current += timedelta(days=1)
        except Exception as exc:
            logger.error(f"Foreclosure scrape failed: {exc}")

        # Ingest from Inbox (Parquet -> SQLite)
        try:
            from src.ingest.inbox_scanner import InboxScanner
            logger.info("Ingesting scraped data from Inbox...")
            InboxScanner(db=db).scan_and_ingest()
        except Exception as e:
            logger.error(f"Ingestion failed: {e}")
        logger.info("=" * 60)
        logger.info("STEP 1.5: SCRAPING TAX DEED AUCTIONS")
        logger.info("=" * 60)

        if skip_tax_deeds:
            logger.info("Skipping tax deed scrape (skip_tax_deeds=True)")
        else:
            try:
                tax_deed_scraper = TaxDeedScraper()
                tax_props = await tax_deed_scraper.scrape_all(scrape_start, scrape_end)
                for p in tax_props:
                    db.upsert_auction(p)
                    # Track status for tax deed cases
                    db.upsert_status(
                        case_number=p.case_number,
                        parcel_id=p.parcel_id,
                        auction_date=p.auction_date,
                        auction_type="TAX_DEED",
                    )
                    db.mark_status_step_complete(
                        p.case_number,
                        "step_auction_scraped",
                        1,
                    )
                logger.success(f"Scraped {len(tax_props)} tax deed auctions")
            except Exception as exc:
                logger.error(f"Tax deed scrape failed: {exc}")

        # CHECKPOINT: Persist auction data before moving to judgment extraction
        db.checkpoint()

    # =========================================================================
    # STEP 2: Judgment Extraction
    # =========================================================================
    def _store_judgment_result(
        db, processor, result, case_number, parcel_id,
        judgment_rows, processed_case_numbers,
    ):
        """Store a successful (or best-effort) judgment extraction to DB."""
        amounts = processor.extract_key_amounts(result)
        db_payload = {
            "plaintiff": result.get("plaintiff"),
            "defendant": "; ".join(
                d.get("name", "") if isinstance(d, dict) else str(d)
                for d in (result.get("defendants") or [])
            ) or result.get("defendant"),
            "foreclosure_type": result.get("foreclosure_type"),
            "judgment_date": result.get("judgment_date"),
            "lis_pendens_date": (result.get("lis_pendens") or {}).get("recording_date"),
            "foreclosure_sale_date": result.get("foreclosure_sale_date"),
            "total_judgment_amount": result.get("total_judgment_amount"),
            "principal_amount": result.get("principal_amount"),
            "interest_amount": result.get("interest_amount"),
            "attorney_fees": result.get("attorney_fees"),
            "court_costs": result.get("court_costs"),
            "original_mortgage_amount": amounts.get("original_mortgage_amount"),
            "original_mortgage_date": amounts.get("original_mortgage_date"),
            "monthly_payment": result.get("monthly_payment"),
            "default_date": result.get("default_date"),
            "extracted_judgment_data": json.dumps(result),
            "raw_judgment_text": result.get("raw_text", ""),
        }
        db.update_judgment_data(case_number, db_payload)
        db.mark_step_complete(case_number, "needs_judgment_extraction")
        db.mark_status_step_complete(case_number, "step_judgment_extracted", 2)

        legal_desc = result.get("legal_description")
        if legal_desc and parcel_id:
            conn = db.connect()
            conn.execute("INSERT OR IGNORE INTO parcels (folio) VALUES (?)", [parcel_id])
            conn.execute(
                "UPDATE parcels SET judgment_legal_description = ? "
                "WHERE folio = ? AND judgment_legal_description IS NULL",
                [legal_desc, parcel_id],
            )

        judgment_rows.append({"case_number": case_number, "parcel_id": parcel_id, **db_payload})
        processed_case_numbers.add(case_number)

    if start_step <= 2:
        logger.info("=" * 60)
        logger.info("STEP 2: DOWNLOADING & EXTRACTING FINAL JUDGMENT DATA")
        logger.info("=" * 60)

        # Mark step_pdf_downloaded for any case that has a PDF on disk but wasn't marked
        try:
            unmarked = db.execute_query(
                """
                SELECT s.case_number, COALESCE(a.parcel_id, a.folio) AS parcel_id
                FROM status s
                JOIN auctions a ON a.case_number = s.case_number
                WHERE s.step_pdf_downloaded IS NULL
                """,
            )
            marked = 0
            for row in unmarked:
                case = row["case_number"]
                pdf_dir = Path(f"data/Foreclosure/{case}/documents")
                if pdf_dir.exists() and any(pdf_dir.glob("*.pdf")):
                    db.mark_status_step_complete(case, "step_pdf_downloaded", 1)
                    marked += 1
            if marked:
                logger.info(f"Marked {marked} cases with existing PDFs as step_pdf_downloaded")
                db.checkpoint()
        except Exception as exc:
            logger.warning(f"PDF status scan failed (non-fatal): {exc}")

        # Backfill: download Final Judgment PDFs for cases missing instrument numbers
        # Uses ORI case-number search API as fallback
        try:
            missing_pdf_cases = db.execute_query(
                """
                SELECT s.case_number
                FROM status s
                WHERE s.step_pdf_downloaded IS NULL
                """,
            )
            if missing_pdf_cases:
                logger.info(
                    f"Attempting ORI case search for {len(missing_pdf_cases)} "
                    f"cases missing judgment PDFs..."
                )
                from src.scrapers.auction_scraper import AuctionScraper
                from playwright.async_api import async_playwright as ap
                import asyncio as _aio

                async def _backfill_pdfs(cases):
                    downloaded = 0
                    async with ap() as pw:
                        _browser = await pw.chromium.launch(headless=True)
                        _ctx = await _browser.new_context(
                            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/119.0.0.0 Safari/537.36",
                            accept_downloads=True,
                        )
                        _page = await _ctx.new_page()
                        from playwright_stealth import Stealth
                        await Stealth().apply_stealth_async(_page)

                        scraper = AuctionScraper()
                        for row in cases:
                            cn = row["case_number"]
                            try:
                                res = await scraper.search_judgment_by_case_number(
                                    _page, cn, ""
                                )
                                if res.get("pdf_path"):
                                    db.mark_status_step_complete(
                                        cn, "step_pdf_downloaded", 1
                                    )
                                    downloaded += 1
                                    logger.info(
                                        f"Backfill: downloaded judgment for {cn}"
                                    )
                                else:
                                    logger.warning(
                                        f"Backfill: no judgment found for {cn}"
                                    )
                            except Exception as be:
                                logger.warning(
                                    f"Backfill: error for {cn}: {be}"
                                )
                        await _browser.close()
                    return downloaded

                loop = _aio.get_event_loop()
                if loop.is_running():
                    import concurrent.futures
                    with concurrent.futures.ThreadPoolExecutor() as pool:
                        backfilled = pool.submit(
                            lambda: _aio.run(_backfill_pdfs(missing_pdf_cases))
                        ).result()
                else:
                    backfilled = loop.run_until_complete(
                        _backfill_pdfs(missing_pdf_cases)
                    )

                if backfilled:
                    logger.info(
                        f"Backfilled {backfilled}/{len(missing_pdf_cases)} "
                        f"judgment PDFs via ORI case search"
                    )
                    db.checkpoint()
        except Exception as exc:
            logger.warning(f"PDF backfill via ORI case search failed (non-fatal): {exc}")

        import time
        import polars as pl

        try:
            judgment_processor = FinalJudgmentProcessor()
            params: list[object] = [retry_failed, max_retries]
            auctions = db.execute_query(
                """
                SELECT a.* FROM auctions a
                LEFT JOIN status s ON s.case_number = a.case_number
                WHERE a.extracted_judgment_data IS NULL
                  AND COALESCE(s.pipeline_status, 'pending') != 'skipped'
                  AND s.step_judgment_extracted IS NULL
                  AND s.step_pdf_downloaded IS NOT NULL
                  AND (COALESCE(s.pipeline_status, 'pending') != 'failed' OR ?)
                  AND COALESCE(s.retry_count, 0) < ?
                """,
                tuple(params),
            )
            logger.info(f"Found {len(auctions)} auctions needing judgment extraction")

            extracted_count = 0
            processed_since_checkpoint = 0
            judgment_rows: list[dict] = []
            thin_cases: list[dict] = []  # Cases needing recovery (thin extraction)
            judgment_dir = Path("data/judgments")
            judgment_dir.mkdir(parents=True, exist_ok=True)
            checkpoint_path = judgment_dir / "judgment_extracts_checkpoint.parquet"
            final_path = judgment_dir / "judgment_extracts_final.parquet"
            last_flush = time.monotonic()
            processed_case_numbers: set[str] = set()

            def _atomic_write_parquet(df: pl.DataFrame, target_path: Path) -> None:
                tmp_path = target_path.with_suffix(target_path.suffix + ".tmp")
                df.write_parquet(tmp_path)
                tmp_path.replace(target_path)

            def _flush_checkpoint() -> None:
                if not judgment_rows:
                    return
                try:
                    df = pl.DataFrame(judgment_rows, infer_schema_length=None)
                    _atomic_write_parquet(df, checkpoint_path)
                    logger.info(f"Wrote judgment checkpoint: {checkpoint_path}")
                except Exception as exc:
                    logger.warning(f"Checkpoint parquet write failed (DB already updated): {exc}")

            if checkpoint_path.exists():
                try:
                    existing_df = pl.read_parquet(checkpoint_path)
                    if "case_number" in existing_df.columns:
                        processed_case_numbers = set(
                            existing_df["case_number"].drop_nulls().unique().to_list()
                        )
                        judgment_rows = existing_df.to_dicts()
                        logger.info(
                            f"Loaded judgment checkpoint with {len(judgment_rows)} rows; "
                            f"skipping {len(processed_case_numbers)} case_numbers"
                        )
                except Exception as exc:
                    logger.warning(f"Failed to load judgment checkpoint: {exc}")
            for auction in auctions:
                case_number = auction["case_number"]
                parcel_id = (auction.get("parcel_id") or "").strip()

                if case_number in processed_case_numbers:
                    continue

                # Primary: data/Foreclosure/{case_number}/documents/ (where auction_scraper saves)
                sanitized_case = case_number.replace("/", "_").replace("\\", "_").replace(":", "_")
                foreclosure_doc_dir = Path("data/Foreclosure") / sanitized_case / "documents"
                pdf_paths = list(foreclosure_doc_dir.glob("final_judgment*.pdf")) if foreclosure_doc_dir.exists() else []

                # Fallback: data/properties/{folio}/documents/
                if not pdf_paths and parcel_id:
                    sanitized_folio = parcel_id.replace("/", "_").replace("\\", "_").replace(":", "_")
                    properties_doc_dir = Path("data/properties") / sanitized_folio / "documents"
                    pdf_paths = list(properties_doc_dir.glob("final_judgment*.pdf")) if properties_doc_dir.exists() else []

                # Legacy fallback
                if not pdf_paths:
                    legacy_path = Path(f"data/pdfs/final_judgments/{case_number}_final_judgment.pdf")
                    if legacy_path.exists():
                        pdf_paths = [legacy_path]

                if not pdf_paths:
                    logger.debug(f"No final judgment PDF on disk for {case_number}")
                    db.mark_status_failed(
                        case_number,
                        "Final judgment PDF not found on disk",
                        error_step=2,
                    )
                    continue

                pdf_path = pdf_paths[0]
                logger.info(f"Processing judgment from {pdf_path.name}...")
                try:
                    result = judgment_processor.process_pdf(str(pdf_path), case_number)
                    if result and not judgment_processor.is_thin_extraction(result):
                        # Good extraction — store immediately
                        _store_judgment_result(
                            db, judgment_processor, result, case_number, parcel_id,
                            judgment_rows, processed_case_numbers,
                        )
                        extracted_count += 1
                    elif result:
                        # Thin extraction — dump PDF text for debugging, queue for recovery
                        logger.warning(
                            f"Thin extraction for {case_number} "
                            f"(no legal_description or mortgage ref) — queuing recovery"
                        )
                        judgment_processor.dump_pdf_text(str(pdf_path), case_number)
                        # Collect party names for ORI recovery search
                        parties = []
                        plaintiff = result.get("plaintiff") or ""
                        if plaintiff:
                            parties.append(plaintiff)
                        for d in result.get("defendants") or []:
                            name = d.get("name", "") if isinstance(d, dict) else str(d)
                            if name:
                                parties.append(name)
                        if parties:
                            thin_cases.append({
                                "case_number": case_number,
                                "parcel_id": parcel_id,
                                "parties": parties,
                                "thin_result": result,
                            })
                        else:
                            # No parties at all — can't recover, store what we have
                            logger.warning(f"No parties extracted for {case_number} — cannot recover")
                            _store_judgment_result(
                                db, judgment_processor, result, case_number, parcel_id,
                                judgment_rows, processed_case_numbers,
                            )
                            extracted_count += 1
                    else:
                        # Vision service returned no structured data
                        logger.warning(f"No structured data extracted for {case_number}")
                        judgment_processor.dump_pdf_text(str(pdf_path), case_number)
                        db.mark_status_failed(
                            case_number,
                            "Vision service returned no structured data",
                            error_step=2,
                        )
                except Exception as exc:
                    logger.warning(f"Failed to process judgment for {case_number}: {exc}")
                    db.mark_status_failed(case_number, str(exc)[:200], error_step=2)

                # Periodic checkpoint every 10 auctions to prevent data loss on kill
                processed_since_checkpoint += 1
                if processed_since_checkpoint >= 10:
                    db.checkpoint()
                    processed_since_checkpoint = 0
                if time.monotonic() - last_flush >= 600:
                    try:
                        _flush_checkpoint()
                    except Exception as exc:
                        logger.error(f"Judgment checkpoint write failed: {exc}")
                        raise
                    last_flush = time.monotonic()

            # ---------------------------------------------------------------
            # Recovery: re-attempt thin extractions via ORI party search
            # ---------------------------------------------------------------
            if thin_cases:
                logger.info(
                    f"RECOVERY: {len(thin_cases)} cases with thin extractions — "
                    f"searching ORI for real Final Judgments"
                )
                import asyncio as _aio
                from src.scrapers.auction_scraper import AuctionScraper

                async def _run_recovery(cases_to_recover):
                    recovered = 0
                    from playwright.async_api import async_playwright
                    async with async_playwright() as p:
                        _browser = await p.chromium.launch(headless=True)
                        _ctx = await _browser.new_context(
                            user_agent=(
                                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) "
                                "Chrome/119.0.0.0 Safari/537.36"
                            ),
                            accept_downloads=True,
                        )
                        _page = await _ctx.new_page()
                        from playwright_stealth import Stealth
                        await Stealth().apply_stealth_async(_page)

                        scraper = AuctionScraper()
                        for case in cases_to_recover:
                            cn = case["case_number"]
                            pid = case["parcel_id"]
                            parties = case["parties"]
                            thin_result = case["thin_result"]
                            try:
                                res = await scraper.recover_judgment_via_party_search(
                                    _page, cn, parties, pid,
                                )
                                recovered_pdf = res.get("pdf_path")
                                if recovered_pdf:
                                    # Re-extract from the real judgment PDF
                                    new_result = judgment_processor.process_pdf(
                                        recovered_pdf, cn,
                                    )
                                    if new_result and not judgment_processor.is_thin_extraction(new_result):
                                        # Tag with recovery metadata
                                        new_result["_recovery"] = {
                                            "original_case": cn,
                                            "recovered_via": res.get("recovered_case_number"),
                                            "original_pdf_was_thin": True,
                                        }
                                        _store_judgment_result(
                                            db, judgment_processor, new_result,
                                            cn, pid, judgment_rows,
                                            processed_case_numbers,
                                        )
                                        recovered += 1
                                        logger.success(
                                            f"Recovery: extracted real judgment for {cn}"
                                        )
                                    else:
                                        # Recovery PDF also thin — store original thin result
                                        logger.warning(
                                            f"Recovery PDF also thin for {cn} — "
                                            f"storing original extraction"
                                        )
                                        if new_result:
                                            judgment_processor.dump_pdf_text(
                                                recovered_pdf, cn,
                                            )
                                        _store_judgment_result(
                                            db, judgment_processor,
                                            new_result or thin_result,
                                            cn, pid, judgment_rows,
                                            processed_case_numbers,
                                        )
                                        extracted_count += 1
                                else:
                                    # Recovery found no PDF — store thin result
                                    logger.warning(
                                        f"Recovery: no real judgment found for {cn}"
                                    )
                                    _store_judgment_result(
                                        db, judgment_processor, thin_result,
                                        cn, pid, judgment_rows,
                                        processed_case_numbers,
                                    )
                                    extracted_count += 1
                            except Exception as re_exc:
                                logger.warning(
                                    f"Recovery failed for {cn}: {re_exc}"
                                )
                                _store_judgment_result(
                                    db, judgment_processor, thin_result,
                                    cn, pid, judgment_rows,
                                    processed_case_numbers,
                                )
                                extracted_count += 1
                        await _browser.close()
                    return recovered

                try:
                    loop = _aio.get_event_loop()
                    if loop.is_running():
                        import concurrent.futures
                        with concurrent.futures.ThreadPoolExecutor() as pool:
                            recovered_count = pool.submit(
                                lambda: _aio.run(_run_recovery(thin_cases))
                            ).result()
                    else:
                        recovered_count = loop.run_until_complete(
                            _run_recovery(thin_cases)
                        )
                    logger.success(
                        f"Recovery: {recovered_count}/{len(thin_cases)} "
                        f"cases recovered with real judgments"
                    )
                except Exception as rec_exc:
                    logger.error(f"Recovery batch failed: {rec_exc}")
                    # Store all thin results so they aren't lost
                    for case in thin_cases:
                        if case["case_number"] not in processed_case_numbers:
                            _store_judgment_result(
                                db, judgment_processor, case["thin_result"],
                                case["case_number"], case["parcel_id"],
                                judgment_rows, processed_case_numbers,
                            )

                db.checkpoint()

            # DB writes already happened per-row above.
            # Write final parquet as a backup/export (best-effort).
            if judgment_rows:
                try:
                    final_df = pl.DataFrame(judgment_rows, infer_schema_length=None)
                    _atomic_write_parquet(final_df, final_path)
                    logger.info(f"Wrote final judgment parquet: {final_path}")
                except Exception as exc:
                    logger.warning(f"Final judgment parquet write failed (DB already updated): {exc}")
            logger.success(f"Extracted data from {extracted_count} Final Judgments")
        except Exception as exc:
            logger.error(f"Judgment extraction failed: {exc}")

        # CHECKPOINT: Persist judgment extractions before bulk enrichment
        db.checkpoint()

    # =========================================================================
    # STEP 2.5: Resolve Missing Parcel IDs
    # =========================================================================
    if start_step <= 3:
        logger.info("=" * 60)
        logger.info("STEP 2.5: Resolve Missing Parcel IDs")
        logger.info("=" * 60)

        try:
            from src.services.parcel_resolver import resolve_missing_parcel_ids

            resolution_stats = resolve_missing_parcel_ids(db)
            logger.success(f"Step 2.5 complete: {resolution_stats}")
        except Exception as exc:
            logger.error(f"Parcel ID resolution failed: {exc}")

        db.checkpoint()

    # =========================================================================
    # STEP 3: Bulk Data Enrichment
    # =========================================================================
    if start_step <= 3:
        logger.info("=" * 60)
        logger.info("STEP 3: BULK DATA ENRICHMENT")
        logger.info("=" * 60)

        try:
            busy_timeout = None
            journal_mode = None
            try:
                busy_timeout = db.conn.execute("PRAGMA busy_timeout").fetchone()[0]
                journal_mode = db.conn.execute("PRAGMA journal_mode").fetchone()[0]
            except Exception as _meta_exc:
                logger.debug(f"Could not read DB settings for bulk enrichment: {_meta_exc}")

            # Retry bulk enrichment with backoff (often fails transiently on locked DB)
            for _attempt in range(3):
                try:
                    enrichment_stats = enrich_auctions_from_bulk(conn=db.conn)
                    break
                except Exception as _e:
                    if "locked" in str(_e).lower() and _attempt < 2:
                        wait = 2 ** _attempt
                        logger.warning(
                            f"Bulk enrichment locked "
                            f"(attempt {_attempt + 1}/3), "
                            f"retrying in {wait}s... "
                            f"(db={db.db_path}, busy_timeout={busy_timeout}, journal_mode={journal_mode})"
                        )
                        await asyncio.sleep(wait)
                        continue
                    raise
            logger.success(f"Bulk enrichment: {enrichment_stats}")
            # Mark all incomplete auctions as bulk enriched
            unenriched = db.execute_query(
                """
                SELECT s.case_number FROM status s
                WHERE s.step_bulk_enriched IS NULL
                  AND COALESCE(s.pipeline_status, 'pending') NOT IN ('completed', 'skipped')
                """
            )
            for auction in unenriched:
                db.mark_status_step_complete(
                    auction["case_number"],
                    "step_bulk_enriched",
                    3,
                )
        except Exception as exc:
            logger.error(
                f"Bulk enrichment failed: {exc} "
                f"(db={db.db_path}, busy_timeout={busy_timeout}, journal_mode={journal_mode})"
            )

        # Update legal descriptions from judgment extractions
        # (column created by PropertyDB._apply_schema_migrations at init)
        try:
            auctions_with_judgment = db.execute_query(
                """
                SELECT parcel_id, extracted_judgment_data FROM auctions
                WHERE parcel_id IS NOT NULL AND extracted_judgment_data IS NOT NULL
                """
            )
            conn = db.connect()
            for row in auctions_with_judgment:
                folio = row["parcel_id"]
                try:
                    judgment_data = json.loads(row["extracted_judgment_data"])
                    legal_desc = judgment_data.get("legal_description")
                    if legal_desc:
                        conn.execute(
                            "INSERT OR IGNORE INTO parcels (folio) VALUES (?)",
                            [folio],
                        )
                        conn.execute(
                            """
                            UPDATE parcels SET
                                judgment_legal_description = ?,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE folio = ? AND judgment_legal_description IS NULL
                            """,
                            [legal_desc, folio],
                        )
                except Exception as exc:
                    logger.debug(f"Could not update judgment legal for {folio}: {exc}")
        except Exception as exc:
            logger.warning(f"Could not update judgment legal descriptions: {exc}")

        # CHECKPOINT: Persist bulk enrichment before parallel processing
        db.checkpoint()

    # =========================================================================
    # STEPS 4+: Parallel Property Enrichment via Orchestrator
    # =========================================================================
    logger.info("=" * 60)
    logger.info("STEPS 4+: PARALLEL PROPERTY ENRICHMENT (Orchestrator)")
    logger.info("=" * 60)

    writer = DatabaseWriter(db=db)
    orchestrator = PipelineOrchestrator(db_writer=writer, db=db, storage=storage)

    await writer.start()
    try:
        await orchestrator.process_auctions(
            start_date=None,
            end_date=None,
            include_failed=retry_failed,
            max_retries=max_retries,
            skip_tax_deeds=skip_tax_deeds,
        )
    finally:
        try:
            await writer.stop()
        finally:
            await orchestrator.ingestion_service.shutdown()

    # CHECKPOINT: Persist all parallel enrichment data before geocoding
    db.checkpoint()

    # =========================================================================
    # STEP 15: Geocode Missing Parcel Coordinates
    # =========================================================================
    if geocode_missing_parcels:
        from src.services.geocoder import geocode_address
        import re

        db.ensure_geocode_columns()

        query = """
            SELECT DISTINCT
                p.folio,
                p.property_address,
                p.city,
                p.zip_code
            FROM parcels p
            JOIN auctions a
              ON COALESCE(a.parcel_id, a.folio) = p.folio
            WHERE (p.latitude IS NULL OR p.longitude IS NULL)
              AND p.property_address IS NOT NULL
              AND p.property_address != ''
              AND LOWER(p.property_address) NOT IN ('unknown', 'n/a', 'none')
        """
        params: list[object] = []
        if geocode_limit is not None:
            query += " LIMIT ?"
            params.append(geocode_limit)

        try:
            rows = db.execute_query(query, tuple(params))
        except Exception as exc:
            logger.error(f"Failed to query parcels needing geocode: {exc}")
            rows = []

        logger.info(f"Found {len(rows)} parcels needing geocode")
        updated = 0
        for row in rows:
            folio = row.get("folio")
            address = (row.get("property_address") or "").strip()
            if not folio or not address:
                continue

            if re.search(r",\s*FL[\s\-]", address, re.IGNORECASE):
                full_address = re.sub(r"FL-\s*", "FL ", address)
            else:
                city = (row.get("city") or "Tampa").strip()
                zip_code = (row.get("zip_code") or "").strip()
                full_address = f"{address}, {city}, FL {zip_code}".strip()

            coords = geocode_address(
                full_address,
                source="parcels",
                folio=str(folio),
            )
            if not coords:
                storage.record_scrape(
                    property_id=str(folio),
                    scraper="geocode",
                    success=False,
                    error="No geocode result",
                    vision_data={"address": full_address},
                    prompt_version="v1",
                )
                continue

            lat, lon = coords
            db.update_parcel_coordinates(str(folio), lat, lon)
            updated += 1
            logger.info(f"Geocoded {folio}: ({lat}, {lon})")
            storage.record_scrape(
                property_id=str(folio),
                scraper="geocode",
                success=True,
                vision_data={"address": full_address, "latitude": lat, "longitude": lon},
                prompt_version="v1",
            )

        logger.success(f"Geocoded {updated}/{len(rows)} parcels")

        # CHECKPOINT: Persist geocoding results
        db.checkpoint()

    # Final checkpoint to ensure all data is persisted
    db.checkpoint()
    logger.success("Full update complete.")
