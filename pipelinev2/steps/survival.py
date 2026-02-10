from __future__ import annotations

import contextlib
import json
from datetime import datetime

import re

from loguru import logger

from src.models.property import Property
from src.scrapers.ori_scraper import ORIScraper
from src.services.lien_survival_analyzer import LienSurvivalAnalyzer

from pipelinev2.state import RunContext, StepResult
from pipelinev2.steps.base import timed_step
from pipelinev2.services import fetch_step_cases, get_db, summarize_step_outcomes

STEP_NAME = "survival"


def _parse_date(value: str | None):
    if not value:
        return None
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _build_defendants(judgment_data: dict) -> list[str]:
    def_names: list[str] = []
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

    defendant = judgment_data.get("defendant")
    if not def_names and defendant:
        def_names = [defendant]

    return def_names


def _parse_recording_reference(ref: str | None) -> dict:
    if not ref:
        return {}
    text = str(ref).strip()
    if not text:
        return {}

    # Book/Page patterns (Book 1234 Page 567, BK 1234 PG 567, 1234/567)
    bkpg = re.search(r"(?:BK|BOOK)\s*#?\s*(\d+)\s*(?:PG|PAGE)\s*(\d+)", text, re.IGNORECASE)
    if bkpg:
        return {"book": bkpg.group(1), "page": bkpg.group(2)}
    slash = re.search(r"\b(\d{3,6})\s*/\s*(\d{1,6})\b", text)
    if slash:
        return {"book": slash.group(1), "page": slash.group(2)}

    # Instrument numbers (usually 8-12 digits)
    inst = re.search(r"(?:INST|INSTRUMENT)\s*#?\s*(\d{8,12})", text, re.IGNORECASE)
    if inst:
        return {"instrument": inst.group(1)}
    digits = re.search(r"\b(\d{8,12})\b", text)
    if digits:
        return {"instrument": digits.group(1)}

    return {}


def _encumbrance_type_for_party(party_type: str | None) -> str:
    if not party_type:
        return "(LIEN) LIEN"
    pt = str(party_type).lower()
    if "mortgage" in pt or "second" in pt or "heloc" in pt:
        return "(MTG) MORTGAGE"
    if "judgment" in pt:
        return "(JUD) JUDGMENT"
    if "hoa" in pt or "condo" in pt or "association" in pt:
        return "(HOA) LIEN"
    if "irs" in pt or "federal" in pt:
        return "(TAX) LIEN"
    if "municipality" in pt:
        return "(MUNI) LIEN"
    return "(LIEN) LIEN"


def _gather_and_analyze_survival_sqlite(db, analyzer: LienSurvivalAnalyzer, prop: Property) -> dict:
    folio = prop.parcel_id
    case_number = prop.case_number

    auction = db.get_auction_by_case(case_number)
    if not auction:
        return {
            "error": "Missing auction record for survival analysis",
            "folio": folio,
            "case_number": case_number,
        }

    encs_rows = db.get_encumbrances_by_folio(folio)
    chain = db.get_chain_of_title(folio)

    current_owner_acq_date = None
    if chain and chain.get("ownership_timeline"):
        acq = chain["ownership_timeline"][-1].get("acquisition_date")
        current_owner_acq_date = _parse_date(acq)

    encumbrances = []
    enc_id_map = {}
    for row in encs_rows:
        rec_date = _parse_date(row.get("recording_date"))
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
            key = f"DTYPE:{rec_date_key}_{row['encumbrance_type']}_{row_id}"
        enc_id_map[key] = row_id

    judgment_data = {}
    raw_judgment = auction.get("extracted_judgment_data")
    if isinstance(raw_judgment, dict):
        judgment_data = raw_judgment
    elif isinstance(raw_judgment, str) and raw_judgment.strip():
        with contextlib.suppress(json.JSONDecodeError, TypeError):
            judgment_data = json.loads(raw_judgment)

    foreclosed_mtg = judgment_data.get("foreclosed_mortgage", {}) or {}
    mtg_book = foreclosed_mtg.get("recording_book")
    mtg_page = foreclosed_mtg.get("recording_page")

    foreclosing_refs = {
        "instrument": foreclosed_mtg.get("instrument_number"),
        "book": mtg_book,
        "page": mtg_page,
    }

    new_encumbrances = []
    if mtg_book and mtg_page and not db.encumbrance_exists(folio, mtg_book, mtg_page):
        mtg_instrument = foreclosed_mtg.get("instrument_number")
        mtg_record_date = foreclosed_mtg.get("recording_date")

        if not mtg_instrument:
            try:
                ori_scraper = ORIScraper()
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
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"Failed to lookup mortgage by book/page: {exc}")

        if mtg_instrument:
            foreclosing_refs["instrument"] = mtg_instrument

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
            "is_inferred": True,
        }
        new_encumbrances.append(new_enc)

        encumbrances.append(
            {
                "encumbrance_type": "(MTG) MORTGAGE",
                "creditor": mtg_creditor,
                "amount": mtg_amount,
                "recording_date": _parse_date(mtg_record_date),
                "book": mtg_book,
                "page": mtg_page,
                "instrument": mtg_instrument,
            }
        )

    if not any(foreclosing_refs.values()):
        foreclosing_refs = None

    # Add encumbrances referenced directly in Final Judgment defendants list.
    defendants = judgment_data.get("defendants", []) or []
    for defendant in defendants:
        if not isinstance(defendant, dict):
            continue
        ref = defendant.get("lien_recording_reference")
        ref_parts = _parse_recording_reference(ref)
        if not ref_parts:
            continue
        book = ref_parts.get("book")
        page = ref_parts.get("page")
        instrument = ref_parts.get("instrument")
        if instrument:
            key = f"INST:{instrument}"
        elif book and page:
            key = f"BKPG:{book}/{page}"
        else:
            continue
        if key in enc_id_map:
            continue

        enc_type = _encumbrance_type_for_party(defendant.get("party_type"))
        creditor = defendant.get("name")
        enc_id = db.insert_encumbrance(
            folio=folio,
            encumbrance_type=enc_type,
            creditor=creditor,
            amount=None,
            recording_date=None,
            book=book,
            page=page,
            instrument=instrument,
            survival_status=None,
            chain_period_id=None,
            is_joined=True,
            is_inferred=True,
        )
        enc_id_map[key] = enc_id
        encumbrances.append(
            {
                "encumbrance_type": enc_type,
                "creditor": creditor,
                "amount": None,
                "recording_date": None,
                "book": book,
                "page": page,
                "instrument": instrument,
            }
        )

    lis_pendens_date = _parse_date(judgment_data.get("lis_pendens_date"))
    def_names = _build_defendants(judgment_data)

    survival_result = analyzer.analyze(
        encumbrances=encumbrances,
        foreclosure_type=auction.get("foreclosure_type") or judgment_data.get("foreclosure_type"),
        lis_pendens_date=lis_pendens_date,
        current_owner_acquisition_date=current_owner_acq_date,
        plaintiff=auction.get("plaintiff"),
        original_mortgage_amount=auction.get("original_mortgage_amount"),
        foreclosing_refs=foreclosing_refs,
        defendants=def_names or None,
    )

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
            enc_orig_id = enc.get("id")
            if instrument:
                key = f"INST:{instrument}"
            elif book and page:
                key = f"BKPG:{book}/{page}"
            elif enc_orig_id:
                key = (
                    f"DTYPE:{rec_date_key}_"
                    f"{enc.get('encumbrance_type') or enc.get('type')}_{enc_orig_id}"
                )
            else:
                key = f"DTYPE:{rec_date_key}_{enc.get('encumbrance_type') or enc.get('type')}"

            db_id = enc_id_map.get(key)
            if db_id:
                upd = {"encumbrance_id": db_id, "status": status_val}
                if enc.get("is_joined") is not None:
                    upd["is_joined"] = enc.get("is_joined")
                if enc.get("is_inferred"):
                    upd["is_inferred"] = True
                updates.append(upd)

    summary = survival_result.get("summary", {})
    return {
        "new_encumbrances": new_encumbrances,
        "updates": updates,
        "summary": summary,
        "folio": folio,
        "case_number": case_number,
    }


def run(context: RunContext) -> StepResult:
    with timed_step(STEP_NAME) as elapsed_ms:
        cases = fetch_step_cases(context, "step_survival_analyzed")
        if not cases:
            return StepResult(step=STEP_NAME, duration_ms=elapsed_ms(), skipped=1)

        db = get_db(context)
        analyzer = LienSurvivalAnalyzer()

        skipped_missing_case = 0
        skipped_missing_parcel = 0
        skipped_existing = 0
        attempted = 0

        for row in cases:
            case_number = row.get("case_number")
            parcel_id = row.get("parcel_id")
            if not case_number or not parcel_id:
                if not case_number:
                    skipped_missing_case += 1
                if not parcel_id:
                    skipped_missing_parcel += 1
                continue

            last_case = db.get_last_analyzed_case(parcel_id)
            has_survival = db.folio_has_survival_analysis(parcel_id)
            has_pending = db.folio_has_unanalyzed_encumbrances(parcel_id)
            if has_survival and not has_pending and last_case == case_number:
                db.mark_step_complete(case_number, "needs_lien_survival")
                db.mark_status_step_complete(case_number, "step_survival_analyzed", 6)
                skipped_existing += 1
                continue

            address = row.get("address") or row.get("property_address") or "Unknown"
            prop = Property(case_number=case_number, parcel_id=parcel_id, address=address)
            attempted += 1

            result = _gather_and_analyze_survival_sqlite(db, analyzer, prop)
            if not result:
                db.mark_status_failed(case_number, "Survival analysis produced no result", 6)
                continue
            if result.get("error"):
                db.mark_status_failed(case_number, str(result["error"])[:200], 6)
                continue

            for update in result.get("updates", []):
                db.update_encumbrance_survival(**update)

            for enc in result.get("new_encumbrances", []):
                db.insert_encumbrance(**enc)

            db.mark_as_analyzed(case_number)
            db.set_last_analyzed_case(parcel_id, case_number)
            db.mark_step_complete(case_number, "needs_lien_survival")
            db.mark_status_step_complete(case_number, "step_survival_analyzed", 6)

        summary = summarize_step_outcomes(
            context,
            [row.get("case_number") for row in cases if row.get("case_number")],
            step_column="step_survival_analyzed",
            error_step=6,
        )

        return StepResult(
            step=STEP_NAME,
            duration_ms=elapsed_ms(),
            processed=len(cases),
            succeeded=summary["completed"],
            failed=summary["failed"],
            skipped=skipped_missing_case + skipped_missing_parcel + skipped_existing,
            artifacts={
                "skipped_missing_case": skipped_missing_case,
                "skipped_missing_parcel": skipped_missing_parcel,
                "skipped_existing": skipped_existing,
                "attempted": attempted,
                "status_summary": summary,
            },
        )
