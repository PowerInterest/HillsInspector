#!/usr/bin/env python3
"""
Rebuild selected DB tables from artifacts in data/properties/*.

Focused on:
- scraper_outputs
- documents
- market_data
- permits
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from src.db.operations import PropertyDB  # noqa: E402

BASE_DIR = Path("data/properties")
DEFAULT_DB = Path("data/property_master.db")

TS_RE = re.compile(r"^(?P<scraper>.+)_(?P<ts>\d{8}_\d{6})")
VISION_RE = re.compile(
    r"^(?P<scraper>.+)_(?P<ts>\d{8}_\d{6})(?:_.+)?_(?P<prompt>v\d+)\.json$"
)
CASE_RE = re.compile(r"[A-Z]{1,}")


def _parse_ts(ts_str: str) -> datetime:
    return datetime.strptime(ts_str, "%Y%m%d_%H%M%S")


def _safe_load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def _unwrap_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(data, dict) and isinstance(data.get("data"), dict):
        return data["data"]
    return data


def _parse_iso_date(value: Optional[str]):
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = f"{value[:-1]}+00:00"
        return datetime.fromisoformat(value).date()
    except Exception:
        return None


def _norm_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text.lower() if text else None


def _norm_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return round(float(str(value).replace(",", "").replace("$", "")), 2)
    except Exception:
        return None


def _iter_files(base: Path, subdir: str, exts: Tuple[str, ...]) -> Iterable[Path]:
    target = base / subdir
    if not target.exists():
        return []
    return (p for p in target.iterdir() if p.is_file() and p.suffix in exts)


def _build_scraper_records(base_dir: Path) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
    records: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    # Raw files
    for raw_path in base_dir.rglob("raw/*"):
        if not raw_path.is_file():
            continue
        m = TS_RE.match(raw_path.name)
        if not m:
            continue
        scraper = m.group("scraper")
        ts = m.group("ts")
        property_id = raw_path.parent.parent.name
        key = (property_id, scraper, ts)
        rec = records.setdefault(
            key,
            {
                "property_id": property_id,
                "scraper": scraper,
                "ts": ts,
                "raw_data_path": None,
                "screenshot_path": None,
                "vision_output_path": None,
                "prompt_version": None,
            },
        )
        rec["raw_data_path"] = f"raw/{raw_path.name}"

    # Screenshots
    for ss_path in base_dir.rglob("screenshots/*"):
        if not ss_path.is_file():
            continue
        m = TS_RE.match(ss_path.name)
        if not m:
            continue
        scraper = m.group("scraper")
        ts = m.group("ts")
        property_id = ss_path.parent.parent.name
        key = (property_id, scraper, ts)
        rec = records.setdefault(
            key,
            {
                "property_id": property_id,
                "scraper": scraper,
                "ts": ts,
                "raw_data_path": None,
                "screenshot_path": None,
                "vision_output_path": None,
                "prompt_version": None,
            },
        )
        rec["screenshot_path"] = f"screenshots/{ss_path.name}"

    # Vision outputs
    for vis_path in base_dir.rglob("vision/*"):
        if not vis_path.is_file():
            continue
        m = VISION_RE.match(vis_path.name)
        if not m:
            continue
        scraper = m.group("scraper")
        ts = m.group("ts")
        prompt = m.group("prompt")
        property_id = vis_path.parent.parent.name
        key = (property_id, scraper, ts)
        rec = records.setdefault(
            key,
            {
                "property_id": property_id,
                "scraper": scraper,
                "ts": ts,
                "raw_data_path": None,
                "screenshot_path": None,
                "vision_output_path": None,
                "prompt_version": None,
            },
        )
        rec["vision_output_path"] = f"vision/{vis_path.name}"
        rec["prompt_version"] = prompt

    return records


def _summary_for_scraper(scraper: str, data: Dict[str, Any]) -> Optional[str]:
    base_scraper = scraper.replace("market_", "", 1) if scraper.startswith("market_") else scraper
    if base_scraper == "fema":
        summary = {
            "flood_zone": data.get("flood_zone"),
            "risk_level": data.get("risk_level"),
            "insurance_required": data.get("insurance_required"),
        }
    elif base_scraper == "permits":
        permits = data.get("permits", [])
        if isinstance(permits, list):
            summary = {
                "total": len(permits),
                "open": sum(
                    1
                    for p in permits
                    if isinstance(p, dict)
                    and str(p.get("status", "")).upper() not in ["FINALED", "CLOSED"]
                ),
            }
        else:
            summary = {"total": 0, "open": 0}
    elif base_scraper == "sunbiz":
        entities = data if isinstance(data, list) else data.get("entities", [])
        if isinstance(entities, list):
            summary = {
                "found": len(entities),
                "active": sum(
                    1
                    for e in entities
                    if isinstance(e, dict)
                    and "ACTIVE" in str(e.get("status", "")).upper()
                ),
            }
        else:
            summary = {"found": 0, "active": 0}
    elif base_scraper in {"realtor", "zillow", "market_realtor", "market_zillow"}:
        summary = {
            "price": data.get("list_price") or data.get("price"),
            "zestimate": data.get("zestimate"),
            "hoa": data.get("hoa_fee") or data.get("hoa_monthly"),
            "status": data.get("listing_status") or data.get("status"),
        }
    else:
        summary = {}
    return json.dumps(summary) if summary else None


def rebuild_scraper_outputs(
    con: duckdb.DuckDBPyConnection,
    base_dir: Path,
    dry_run: bool,
) -> Dict[str, int]:
    records = _build_scraper_records(base_dir)
    existing = con.execute(
        """
        SELECT property_id, scraper, scraped_at, raw_data_path, screenshot_path, vision_output_path
        FROM scraper_outputs
        """
    ).fetchall()
    existing_keys = {
        (row[0], row[1], row[2], row[3], row[4], row[5]) for row in existing
    }

    inserted = 0
    skipped = 0
    for rec in records.values():
        ts = rec["ts"]
        try:
            scraped_at = _parse_ts(ts)
        except Exception:
            scraped_at = None
        processed_at = scraped_at if rec["vision_output_path"] else None

        extracted_summary = None
        data_source = None
        if rec["raw_data_path"]:
            data_source = base_dir / rec["property_id"] / rec["raw_data_path"]
        elif rec["vision_output_path"]:
            data_source = base_dir / rec["property_id"] / rec["vision_output_path"]

        if data_source and data_source.exists():
            payload = _safe_load_json(data_source)
            if isinstance(payload, dict):
                extracted_summary = _summary_for_scraper(rec["scraper"], payload)

        key = (
            rec["property_id"],
            rec["scraper"],
            scraped_at,
            rec["raw_data_path"],
            rec["screenshot_path"],
            rec["vision_output_path"],
        )
        if key in existing_keys:
            skipped += 1
            continue

        if not dry_run:
            con.execute(
                """
                INSERT INTO scraper_outputs (
                    property_id, scraper,
                    scraped_at, processed_at,
                    screenshot_path, vision_output_path, raw_data_path,
                    prompt_version, extraction_success, error_message,
                    extracted_summary, source_url, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    rec["property_id"],
                    rec["scraper"],
                    scraped_at,
                    processed_at,
                    rec["screenshot_path"],
                    rec["vision_output_path"],
                    rec["raw_data_path"],
                    rec["prompt_version"],
                    True,
                    None,
                    extracted_summary,
                    None,
                ],
            )
        inserted += 1

    return {"identified": len(records), "inserted": inserted, "skipped": skipped}


def _parse_document_filename(filename: str) -> Dict[str, Optional[str]]:
    stem = Path(filename).stem
    parts = stem.split("_")
    doc_type = stem
    instrument_number = None
    case_number = None

    if stem.startswith("final_judgment_"):
        doc_type = "final_judgment"
        token = stem[len("final_judgment_") :]
        if CASE_RE.search(token):
            case_number = token
        else:
            instrument_number = token
        return {
            "document_type": doc_type,
            "instrument_number": instrument_number,
            "case_number": case_number,
        }

    if stem.startswith("unknown_") and len(parts) >= 3:
        doc_type = "_".join(parts[1:-1]) if len(parts) > 2 else parts[1]
        if parts[-1].isdigit():
            instrument_number = parts[-1]
        return {
            "document_type": doc_type,
            "instrument_number": instrument_number,
            "case_number": None,
        }

    if len(parts) > 1 and parts[-1].isdigit():
        doc_type = "_".join(parts[:-1])
        instrument_number = parts[-1]

    return {
        "document_type": doc_type,
        "instrument_number": instrument_number,
        "case_number": case_number,
    }


def rebuild_documents(
    con: duckdb.DuckDBPyConnection,
    base_dir: Path,
    dry_run: bool,
) -> Dict[str, int]:
    existing_files = {
        (row[0], row[1])
        for row in con.execute(
            "SELECT folio, file_path FROM documents WHERE file_path IS NOT NULL"
        ).fetchall()
    }
    existing_instruments = {
        (row[0], row[1])
        for row in con.execute(
            "SELECT folio, instrument_number FROM documents WHERE instrument_number IS NOT NULL"
        ).fetchall()
    }

    inserted = 0
    skipped = 0
    identified = 0
    for doc_path in base_dir.rglob("documents/*"):
        if not doc_path.is_file():
            continue
        if doc_path.suffix.lower() not in {".pdf", ".png", ".jpg", ".jpeg"}:
            continue
        property_id = doc_path.parent.parent.name
        rel_path = f"documents/{doc_path.name}"
        identified += 1

        meta = _parse_document_filename(doc_path.name)
        case_number = meta["case_number"]
        if property_id.startswith("unknown_case_") and not case_number:
            case_number = property_id.replace("unknown_case_", "", 1)

        key_file = (property_id, rel_path)
        key_inst = (
            property_id,
            meta["instrument_number"] if meta["instrument_number"] else None,
        )
        if key_file in existing_files or key_inst in existing_instruments:
            skipped += 1
            continue

        if not dry_run:
            con.execute(
                """
                INSERT INTO documents (
                    folio, case_number, document_type, file_path, ocr_text,
                    extracted_data, recording_date, book, page,
                    instrument_number, party1, party2, legal_description,
                    party2_resolution_method, is_self_transfer, self_transfer_type,
                    sales_price, page_count, ori_uuid, ori_id, book_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    property_id,
                    case_number,
                    meta["document_type"],
                    rel_path,
                    None,
                    json.dumps({}),
                    None,
                    None,
                    None,
                    meta["instrument_number"],
                    None,
                    None,
                    None,
                    None,
                    False,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ],
            )
        inserted += 1

    return {"identified": identified, "inserted": inserted, "skipped": skipped}


def rebuild_market_data(
    con: duckdb.DuckDBPyConnection,
    base_dir: Path,
    dry_run: bool,
) -> Dict[str, int]:
    def market_signature(
        folio: str,
        source: str,
        capture_date,
        listing_status,
        list_price,
        zestimate,
        rent_estimate,
        hoa_monthly,
        days_on_market,
    ):
        return (
            folio,
            source,
            capture_date,
            _norm_text(listing_status),
            _norm_float(list_price),
            _norm_float(zestimate),
            _norm_float(rent_estimate),
            _norm_float(hoa_monthly),
            days_on_market if days_on_market is None else int(days_on_market),
        )

    existing = {
        market_signature(
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[7],
            row[8],
        )
        for row in con.execute(
            """
            SELECT folio, source, capture_date, listing_status, list_price,
                   zestimate, rent_estimate, hoa_monthly, days_on_market
            FROM market_data
            """
        ).fetchall()
    }

    inserted = 0
    skipped = 0
    identified = 0

    seen = set()

    def handle_market_record(
        property_id: str,
        source: str,
        capture_date,
        payload: Dict[str, Any],
        raw_json: Dict[str, Any],
        screenshot_path: Optional[str],
    ):
        nonlocal inserted, skipped, identified
        identified += 1
        signature = market_signature(
            property_id,
            source,
            capture_date,
            payload.get("listing_status") or payload.get("status"),
            payload.get("list_price") or payload.get("price"),
            payload.get("zestimate"),
            payload.get("rent_zestimate") or payload.get("rent_estimate"),
            payload.get("hoa_fee") or payload.get("hoa_monthly"),
            payload.get("days_on_market"),
        )
        if signature in existing or signature in seen:
            skipped += 1
            return
        seen.add(signature)

        if not dry_run:
            con.execute(
                """
                INSERT INTO market_data (
                    folio, source, capture_date, listing_status, list_price,
                    zestimate, rent_estimate, hoa_monthly, days_on_market,
                    price_history, raw_json, screenshot_path
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    property_id,
                    source,
                    capture_date,
                    payload.get("listing_status") or payload.get("status"),
                    payload.get("list_price") or payload.get("price"),
                    payload.get("zestimate"),
                    payload.get("rent_zestimate") or payload.get("rent_estimate"),
                    payload.get("hoa_fee") or payload.get("hoa_monthly"),
                    payload.get("days_on_market"),
                    json.dumps(payload.get("price_history", [])),
                    json.dumps(raw_json),
                    screenshot_path,
                ],
            )
        inserted += 1

    # Vision outputs
    for vis_path in base_dir.rglob("vision/market_*_v*.json"):
        if not vis_path.is_file():
            continue
        m = VISION_RE.match(vis_path.name)
        if not m:
            continue
        scraper = m.group("scraper")
        ts = m.group("ts")
        property_id = vis_path.parent.parent.name
        source = "Realtor" if "realtor" in scraper else "Zillow"

        try:
            capture_date = _parse_ts(ts).date()
        except Exception:
            capture_date = None

        data = _safe_load_json(vis_path)
        if not isinstance(data, dict):
            continue
        payload = _unwrap_payload(data)

        # Prefer extracted_at when present
        extracted_at = _parse_iso_date(data.get("extracted_at"))
        if extracted_at:
            capture_date = extracted_at

        screenshot_path = None
        if isinstance(data.get("screenshot"), str) and data["screenshot"]:
            screenshot_path = data["screenshot"]
        else:
            expected = vis_path.parent.parent / "screenshots" / f"{scraper}_{ts}_listing.png"
            if expected.exists():
                screenshot_path = f"screenshots/{expected.name}"

        handle_market_record(
            property_id,
            source,
            capture_date,
            payload,
            data,
            screenshot_path,
        )

    # Raw outputs (if any exist)
    for raw_path in base_dir.rglob("raw/market_*.json"):
        if not raw_path.is_file():
            continue
        m = TS_RE.match(raw_path.name)
        if not m:
            continue
        scraper = m.group("scraper")
        ts = m.group("ts")
        property_id = raw_path.parent.parent.name
        source = "Realtor" if "realtor" in scraper else "Zillow"
        try:
            capture_date = _parse_ts(ts).date()
        except Exception:
            capture_date = None
        data = _safe_load_json(raw_path)
        if not isinstance(data, dict):
            continue
        payload = _unwrap_payload(data)
        handle_market_record(
            property_id,
            source,
            capture_date,
            payload,
            data,
            None,
        )

    return {"identified": identified, "inserted": inserted, "skipped": skipped}


def rebuild_permits(
    con: duckdb.DuckDBPyConnection,
    base_dir: Path,
    dry_run: bool,
) -> Dict[str, int]:
    def permit_key(
        permit_number: Optional[str],
        folio: str,
        issue_date,
        status: Optional[str],
        permit_type: Optional[str],
        description: Optional[str],
    ):
        if permit_number:
            return ("num", permit_number.strip().upper())
        return (
            "sig",
            folio,
            issue_date,
            _norm_text(status),
            _norm_text(permit_type),
            _norm_text(description),
        )

    existing = {
        permit_key(row[1], row[0], row[2], row[3], row[4], row[5])
        for row in con.execute(
            """
            SELECT folio, permit_number, issue_date, status, permit_type, description
            FROM permits
            """
        ).fetchall()
    }

    inserted = 0
    skipped = 0
    identified = 0

    seen = set()

    def handle_permit_record(property_id: str, permit: Dict[str, Any]):
        nonlocal inserted, skipped, identified
        if not isinstance(permit, dict):
            return
        identified += 1
        permit_number = permit.get("number") or permit.get("permit_number")
        issue_date = permit.get("issue_date")
        try:
            issue_date = (
                datetime.fromisoformat(issue_date).date()
                if isinstance(issue_date, str)
                else None
            )
        except Exception:
            issue_date = None

        key = permit_key(
            permit_number,
            property_id,
            issue_date,
            permit.get("status"),
            permit.get("type") or permit.get("permit_type"),
            permit.get("description") or permit.get("work_description"),
        )
        if key in existing or key in seen:
            skipped += 1
            return
        seen.add(key)

        if not dry_run:
            con.execute(
                """
                INSERT INTO permits (
                    folio, permit_number, issue_date, status, permit_type,
                    description, contractor, estimated_cost, url, noc_instrument
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    property_id,
                    permit_number,
                    issue_date,
                    permit.get("status"),
                    permit.get("type") or permit.get("permit_type"),
                    permit.get("description") or permit.get("work_description"),
                    permit.get("contractor"),
                    permit.get("estimated_cost"),
                    permit.get("url"),
                    permit.get("noc_instrument"),
                ],
            )
        inserted += 1

    # Vision outputs (permit scraper)
    for vis_path in base_dir.rglob("vision/permits_*_v*.json"):
        if not vis_path.is_file():
            continue
        property_id = vis_path.parent.parent.name
        data = _safe_load_json(vis_path)
        if not isinstance(data, dict):
            continue
        for permit in data.get("permits") or []:
            handle_permit_record(property_id, permit)

    # Raw outputs from HCPA GIS property_details (permits list)
    for raw_path in base_dir.rglob("raw/hcpa_gis_*_property_details.json"):
        if not raw_path.is_file():
            continue
        property_id = raw_path.parent.parent.name
        data = _safe_load_json(raw_path)
        if not isinstance(data, dict):
            continue
        for permit in data.get("permits") or []:
            if isinstance(permit, dict):
                handle_permit_record(
                    property_id,
                    {
                        "permit_number": permit.get("permit_number"),
                        "permit_type": permit.get("permit_type"),
                        "url": permit.get("link"),
                    },
                )

    # Raw permits payloads (if any exist)
    for raw_path in base_dir.rglob("raw/permits_*.json"):
        if not raw_path.is_file():
            continue
        property_id = raw_path.parent.parent.name
        data = _safe_load_json(raw_path)
        if not isinstance(data, dict):
            continue
        for permit in data.get("permits") or []:
            handle_permit_record(property_id, permit)

    return {"identified": identified, "inserted": inserted, "skipped": skipped}


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild DB tables from data/properties artifacts.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB))
    parser.add_argument("--base-dir", default=str(BASE_DIR))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-scraper-outputs", action="store_true")
    parser.add_argument("--skip-documents", action="store_true")
    parser.add_argument("--skip-market-data", action="store_true")
    parser.add_argument("--skip-permits", action="store_true")
    parser.add_argument("--reset-scraper-outputs", action="store_true")
    parser.add_argument("--reset-documents", action="store_true")
    parser.add_argument("--reset-market-data", action="store_true")
    parser.add_argument("--reset-permits", action="store_true")
    parser.add_argument("--update-status", action="store_true")
    args = parser.parse_args()

    base_dir = Path(args.base_dir)
    db_path = Path(args.db_path)

    con = duckdb.connect(str(db_path), read_only=False)

    if args.reset_scraper_outputs:
        con.execute("DELETE FROM scraper_outputs")
    if args.reset_documents:
        con.execute("DELETE FROM documents")
    if args.reset_market_data:
        con.execute("DELETE FROM market_data")
    if args.reset_permits:
        con.execute("DELETE FROM permits")

    results = {}
    if not args.skip_scraper_outputs:
        results["scraper_outputs"] = rebuild_scraper_outputs(con, base_dir, args.dry_run)
    if not args.skip_documents:
        results["documents"] = rebuild_documents(con, base_dir, args.dry_run)
    if not args.skip_market_data:
        results["market_data"] = rebuild_market_data(con, base_dir, args.dry_run)
    if not args.skip_permits:
        results["permits"] = rebuild_permits(con, base_dir, args.dry_run)

    if not args.dry_run:
        con.execute("CHECKPOINT")
    con.close()

    if args.update_status and not args.dry_run:
        with PropertyDB(db_path=str(db_path)) as db:
            count = db.initialize_status_from_auctions()
            db.checkpoint()
        print(f"status: updated ({count} records)")

    for table, stats in results.items():
        print(f"{table}: {stats}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
