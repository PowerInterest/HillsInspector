import asyncio
import json
import contextlib
from datetime import date, datetime
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
from src.scrapers.fema_flood_scraper import FEMAFloodChecker
from src.services.ingestion_service import IngestionService
from src.services.lien_survival_analyzer import LienSurvivalAnalyzer
from src.services.homeharvest_service import HomeHarvestService

# Database & Storage
from src.db.operations import PropertyDB
from src.db.writer import DatabaseWriter
from src.services.scraper_storage import ScraperStorage
from src.utils.legal_description import parse_legal_description, generate_search_permutations, combine_legal_fields

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

class PipelineOrchestrator:
    """
    Orchestrates the scraping and analysis pipeline.
    """
    
    def __init__(self, db_writer: DatabaseWriter, max_concurrent_properties: int = 15, db: Optional[PropertyDB] = None, storage: Optional[ScraperStorage] = None):
        self.db_writer = db_writer
        self.db = db or PropertyDB() # Read-only access for status checks
        self.storage = storage or ScraperStorage()
        
        # Services
        self.tax_scraper = TaxScraper(storage=self.storage)
        self.permit_scraper = PermitScraper(headless=True, use_vision=True, storage=self.storage)
        self.market_scraper = MarketScraper(headless=True, storage=self.storage)
        self.sunbiz_scraper = SunbizScraper(headless=True, storage=self.storage)
        self.fema_checker = FEMAFloodChecker(storage=self.storage)
        
        # Heavy Services (Injected with db_writer for serialization)
        self.ingestion_service = IngestionService(db_writer=self.db_writer)
        self.survival_analyzer = LienSurvivalAnalyzer()
        self.homeharvest_service = HomeHarvestService()
        
        # Concurrency Control
        self.property_semaphore = asyncio.Semaphore(max_concurrent_properties)
        self.market_semaphore = asyncio.Semaphore(3)
        self.tax_semaphore = asyncio.Semaphore(5)
        self.permit_semaphore = asyncio.Semaphore(5)
        self.hcpa_semaphore = asyncio.Semaphore(5)
        self.sunbiz_semaphore = asyncio.Semaphore(5)
        self.fema_semaphore = asyncio.Semaphore(10)
        self.homeharvest_semaphore = asyncio.Semaphore(1)

    # ... (Previous methods unchanged) ...

    def _gather_and_analyze_survival(self, prop: Property) -> Dict[str, Any]:
        """
        Synchronous worker method for Survival Analysis.
        Reads DB, runs logic, returns updates to be applied.
        """
        folio = prop.parcel_id
        case_number = prop.case_number
        
        # 1. Gather Data
        auction = self.db.get_auction_by_case(case_number)
        if not auction: return {}
        
        encs_rows = self.db.get_encumbrances_by_folio(folio)
        chain = self.db.get_chain_of_title(folio)
        
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
            key = row.get("instrument") or f"{rec_date}_{row['encumbrance_type']}"
            enc_id_map[key] = row["id"]

        # 2. Identify Foreclosing Mortgage
        judgment_data = {}
        if auction.get("extracted_judgment_data"):
             with contextlib.suppress(json.JSONDecodeError):
                 judgment_data = json.loads(auction["extracted_judgment_data"])

        foreclosed_mtg = judgment_data.get("foreclosed_mortgage", {})
        mtg_book = foreclosed_mtg.get("recording_book")
        mtg_page = foreclosed_mtg.get("recording_page")
        
        foreclosing_refs = {
            "instrument": foreclosed_mtg.get("instrument_number"),
            "book": mtg_book,
            "page": mtg_page
        }
        
        new_encumbrances = [] # List of dicts to insert
        
        if mtg_book and mtg_page and not self.db.encumbrance_exists(folio, mtg_book, mtg_page):
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
                "encumbrance_type": "(MTG) MORTGAGE",
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
                 "encumbrance_type": "(MTG) MORTGAGE",
                 "creditor": mtg_creditor,
                 "amount": mtg_amount,
                 "recording_date": rec_date_parsed,
                 "book": mtg_book,
                 "page": mtg_page,
                 "instrument": mtg_instrument,
            })

        # 3. Analyze
        lis_pendens_date = None
        lp_str = judgment_data.get("lis_pendens_date")
        if lp_str:
            with contextlib.suppress(ValueError):
                lis_pendens_date = datetime.strptime(lp_str, "%Y-%m-%d").date()

        # Extract defendant names list from judgment data for "Joined" check (matches old pipeline)
        def_names = []
        if judgment_data.get("defendants"):
            defs = judgment_data.get("defendants", [])
            if isinstance(defs, list):
                def_names = [d.get("name") for d in defs if d.get("name")]

        # Fallback to single string if list missing (matches old pipeline)
        if not def_names and judgment_data.get("defendant"):
            def_names = [judgment_data.get("defendant")]

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
                key = enc.get("instrument") or f"{enc.get('recording_date')}_{enc.get('type')}"
                # If checking against new_encumbrances, they don't have DB IDs yet.
                # But they are "FORECLOSING" status already.
                # Only update EXISTING DB records.
                db_id = enc_id_map.get(key)
                if db_id:
                     upd = {"encumbrance_id": db_id, "status": status_val}
                     if enc.get("is_joined") is not None: upd["is_joined"] = enc.get("is_joined")
                     if enc.get("is_inferred"): upd["is_inferred"] = True
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

    async def process_auctions(self, start_date: date, end_date: date):
        """
        Main entry point. Enriches auctions within the date range.
        """
        logger.info(f"Starting orchestration for {start_date} to {end_date}")
        
        auctions = self.db.get_auctions_by_date_range(start_date, end_date)
        logger.info(f"Found {len(auctions)} auctions to process")
        
        await self._process_batch(auctions)
        
        logger.success("Orchestration complete")

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
        
        # Determine address early
        address = auction_dict.get('address') or auction_dict.get('location_address') or auction_dict.get('property_address') or "Unknown"

        if not parcel_id:
            logger.warning(f"Skipping enrichment: No parcel_id for case {case_number}")
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

        # Check for invalid folios (mobile homes, "Property Appraiser", etc.)
        if not is_valid_folio(parcel_id):
            if prop.plaintiff or prop.defendant:
                logger.info(f"Invalid folio '{parcel_id}' for case {case_number}, trying party-based ORI search")
                try:
                    # Run party-based ingestion in executor (it's synchronous)
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(
                        None,
                        lambda: self.ingestion_service.ingest_property_by_party(prop, prop.plaintiff, prop.defendant)
                    )
                    # Mark step complete
                    await self.db_writer.enqueue("generic_call", {
                        "func": self.db.mark_step_complete,
                        "args": [case_number, "needs_ori_ingestion"]
                    })
                except Exception as e:
                    logger.error(f"Party-based ingestion failed for {case_number}: {e}")
            else:
                logger.warning(f"Invalid folio '{parcel_id}' and no party data. Skipping ORI.")
                await self.db_writer.enqueue("generic_call", {
                    "func": self.db.mark_step_complete,
                    "args": [case_number, "needs_ori_ingestion"]
                })
            return

        if prop.address == "Unknown":
            logger.warning(f"Skipping enrichment for {parcel_id}: No address")
            return

        logger.info(f"Enriching {parcel_id} ({prop.address})")

        # PHASE 1: Independent Parallel Scrapers (Data Gathering)
        # These don't depend on each other and can run immediately
        logger.info(f"Phase 1: Starting parallel gather for {parcel_id}")
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._run_tax_scraper(parcel_id, prop.address))
            tg.create_task(self._run_market_scraper(parcel_id, prop.address))
            tg.create_task(self._run_homeharvest(prop))
            tg.create_task(self._run_fema_checker(parcel_id, prop.address))
            tg.create_task(self._run_sunbiz_scraper(parcel_id, prop.owner_name))
            tg.create_task(self._run_hcpa_gis(parcel_id))

        # PHASE 2: ORI Ingestion (Depends on Legal Description from HCPA/Bulk)
        # HCPA GIS (Phase 1) might have updated legal description in DB
        logger.info(f"Phase 2: Starting ORI Ingestion for {parcel_id}")

        # Skip logic: folio has chain AND same case number (matches old pipeline)
        last_case = self.db.get_last_analyzed_case(parcel_id)
        if self.db.folio_has_chain_of_title(parcel_id) and last_case == case_number:
            logger.debug(f"Skipping ORI for {parcel_id} - already analyzed for {case_number}")
            await self.db_writer.enqueue("generic_call", {
                "func": self.db.mark_step_complete,
                "args": [case_number, "needs_ori_ingestion"]
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
                # No legal description available - mark for manual review (matches old pipeline)
                logger.warning(f"No usable legal description for {parcel_id} (case {case_number}), marking for manual review")
                await self.db_writer.enqueue("generic_call", {
                    "func": self.db.mark_hcpa_scrape_failed,
                    "args": [case_number, "No usable legal description (HCPA/judgment/bulk)"],
                })
                # Mark complete so we don't loop forever
                await self.db_writer.enqueue("generic_call", {
                    "func": self.db.mark_step_complete,
                    "args": [case_number, "needs_ori_ingestion"]
                })
            else:
                # Build search terms (matches old pipeline logic exactly)
                prop.legal_description = primary_legal
                parsed = parse_legal_description(primary_legal)
                terms = generate_search_permutations(parsed)

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
                    prefix = primary_legal.upper().strip()[:60]
                    if prefix:
                        terms.insert(0, f"{prefix}*")

                prop.legal_search_terms = terms
                logger.info(f"  Legal ({legal_source}): {primary_legal}")
                logger.info(f"  Search terms: {terms}")

                await self._run_ori_ingestion(prop)

                # Mark step complete after successful ingestion (matches old pipeline)
                await self.db_writer.enqueue("generic_call", {
                    "func": self.db.mark_step_complete,
                    "args": [case_number, "needs_ori_ingestion"]
                })

        # PHASE 3: Dependent Parallel Analysis (Needs ORI Data)
        # Permits needs NOCs (from ORI)
        # Survival needs Encumbrances (from ORI)
        logger.info(f"Phase 3: Starting Analysis for {parcel_id}")
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._run_permit_scraper(parcel_id, address))
            tg.create_task(self._run_survival_analysis(prop)) # Needs full prop context

    # -------------------------------------------------------------------------
    # Individual Execution Wrappers
    # -------------------------------------------------------------------------

    async def _run_tax_scraper(self, parcel_id: str, address: str):
        if self.db.folio_has_tax_data(parcel_id): return
        async with self.tax_semaphore:
            try:
                tax_status = await self.tax_scraper.scrape_tax_status(parcel_id, address)
                # Derive status strings
                status_str = "PAID" if tax_status.paid_in_full else ("DELINQUENT" if tax_status.amount_due > 0 else "UNKNOWN")
                has_warrant = len(tax_status.certificates) > 0
                
                await self.db_writer.enqueue("update_tax_status", {"folio": parcel_id, "tax_status": status_str, "tax_warrant": has_warrant})
            except Exception as e:
                logger.warning(f"Tax scraper failed: {e}")

    async def _run_market_scraper(self, parcel_id: str, address: str):
        # Check if we already have recent market data
        if self.db.folio_has_realtor_data(parcel_id): 
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
                     state_zip = parts[2].strip().split(" ")
                     if len(state_zip) > 0: state = state_zip[0]
                     if len(state_zip) > 1: zip_code = state_zip[1] 

                # get_listing_details now tries both Zillow and Realtor
                listing = await self.market_scraper.get_listing_details(
                    address=street, city=city, state=state, zip_code=zip_code, property_id=parcel_id
                )
                
                if listing and (listing.price or listing.status != "Unknown"):
                    # Save consolidated market data
                    await self.db_writer.enqueue("save_market_data", {
                        "folio": parcel_id, 
                        "source": "Consolidated", 
                        "data": listing.dict(), 
                        "screenshot_path": getattr(listing, 'screenshot_path', None)
                    })
                    logger.success(f"Consolidated market data saved for {parcel_id}")
            except Exception as e:
                logger.warning(f"Market scraper failed for {parcel_id}: {e}")

    async def _run_homeharvest(self, prop: Property):
        """Phase 1: Run HomeHarvest Enrichment."""
        if self.db.folio_has_homeharvest_data(prop.parcel_id):
            return

        async with self.homeharvest_semaphore:
            logger.info(f"Running HomeHarvest for {prop.address}")
            loop = asyncio.get_running_loop()
            try:
                # Wrap the synchronous fetch_and_save
                props = [{
                    "folio": prop.parcel_id,
                    "location": prop.address,
                    "case_number": prop.case_number
                }]
                await loop.run_in_executor(
                    None,
                    lambda: self.homeharvest_service.fetch_and_save(props)
                )
            except Exception as e:
                logger.error(f"HomeHarvest failed for {prop.parcel_id}: {e}")

    async def _run_fema_checker(self, parcel_id: str, address: str):
        # Check cache via storage
        async with self.fema_semaphore:
            try:
                # Fetch property to get coords (returns dict, not object)
                prop = self.db.get_property(parcel_id)
                if not prop:
                    return

                lat = prop.get("latitude") if isinstance(prop, dict) else getattr(prop, "latitude", None)
                lon = prop.get("longitude") if isinstance(prop, dict) else getattr(prop, "longitude", None)

                if not lat or not lon:
                    return

                result = self.fema_checker.get_flood_zone_for_property(parcel_id, lat, lon)
                if result:
                    await self.db_writer.enqueue("save_flood_data", {
                        "folio": parcel_id,
                        "flood_zone": result.flood_zone,
                        "flood_risk": result.risk_level,
                        "insurance_required": result.insurance_required
                    })
            except Exception as e:
                logger.warning(f"FEMA failed: {e}")

    async def _run_sunbiz_scraper(self, parcel_id: str, owner_name: str):
        if not owner_name: return
        # Sunbiz checks owner name
        async with self.sunbiz_semaphore:
            try:
                await self.sunbiz_scraper.search_for_property(parcel_id, owner_name)
            except Exception as e:
                logger.warning(f"Sunbiz failed: {e}")

    async def _run_hcpa_gis(self, parcel_id: str):
        if self.db.folio_has_sales_history(parcel_id): return
        async with self.hcpa_semaphore:
            try:
                result = await scrape_hcpa_property(parcel_id=parcel_id)
                
                # Check errors
                if result.get("error"):
                    return

                # Save Sales History via Writer
                if result.get("sales_history"):
                     await self.db_writer.enqueue("generic_call", {
                         "func": self.db.save_sales_history_from_hcpa,
                         "args": [parcel_id, result["sales_history"]]
                     })

                # Save new legal description
                if result.get("legal_description"):
                     await self.db_writer.enqueue("generic_call", {
                         "func": self.db.update_legal_description,
                         "args": [parcel_id, result["legal_description"]]
                     })

            except Exception as e:
                 logger.warning(f"HCPA GIS failed: {e}")

    async def _run_ori_ingestion(self, prop: Property):
        # IngestionService manages its own internal semaphores/concurrency via db_writer
        try:
            await self.ingestion_service.ingest_property_async(prop)
        except Exception as e:
            logger.error(f"ORI Ingestion failed: {e}")

    async def _run_permit_scraper(self, parcel_id: str, address: str):
        if self.db.folio_has_permits(parcel_id): return
        async with self.permit_semaphore:
             try:
                permits = await self.permit_scraper.get_permits_for_property(parcel_id, address, "Tampa")
                if permits:
                    await self.db_writer.enqueue("save_permits", {"folio": parcel_id, "permits": permits})
             except Exception as e:
                 logger.error(f"Permit scraper failed: {e}")

    async def _run_survival_analysis(self, prop: Property):
         try:
             # Skip logic: folio has survival AND same case number (matches old pipeline)
             last_case = self.db.get_last_analyzed_case(prop.parcel_id)
             if self.db.folio_has_survival_analysis(prop.parcel_id) and last_case == prop.case_number:
                 logger.debug(f"Skipping survival for {prop.parcel_id} - already analyzed for {prop.case_number}")
                 await self.db_writer.enqueue("generic_call", {
                     "func": self.db.mark_step_complete,
                     "args": [prop.case_number, "needs_lien_survival"]
                 })
                 return

             # Logic is heavy and synchronous (DB reads, potential ORI lookup). Run in executor.
             loop = asyncio.get_running_loop()
             result = await loop.run_in_executor(None, self._gather_and_analyze_survival, prop)

             if not result:
                 return

             # Persist results (Writes via db_writer)
             # Update encumbrances
             updates = result.get("updates", [])
             for update in updates:
                 await self.db_writer.enqueue("generic_call", {
                     "func": self.db.update_encumbrance_survival,
                     "kwargs": update
                 })

             # Create foreclosing mortgage if identified/missing
             new_encs = result.get("new_encumbrances", [])
             for enc in new_encs:
                 await self.db_writer.enqueue("generic_call", {
                     "func": self.db.insert_encumbrance,
                     "kwargs": enc
                 })

             # Mark as analyzed and record case number (CRITICAL - was missing!)
             await self.db_writer.enqueue("generic_call", {
                 "func": self.db.mark_as_analyzed,
                 "args": [prop.case_number]
             })
             await self.db_writer.enqueue("generic_call", {
                 "func": self.db.set_last_analyzed_case,
                 "args": [prop.parcel_id, prop.case_number]
             })

             # Mark step complete
             await self.db_writer.enqueue("generic_call", {
                 "func": self.db.mark_step_complete,
                 "args": [prop.case_number, "needs_lien_survival"]
             })

             # Log summary (matches old pipeline)
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
