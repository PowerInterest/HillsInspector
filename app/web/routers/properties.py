"""
Property detail routes.
"""

import contextlib
import json
import re
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from pathlib import Path
from typing import Any

from loguru import logger
from app.web.pg_database import get_pg_queries
from app.web.template_filters import get_templates
from src.utils.time import today_local
from sqlalchemy import text as sa_text

from sunbiz.db import get_engine, resolve_pg_dsn

router = APIRouter()

templates = get_templates()
UTC_TZ = getattr(datetime, "UTC", timezone(timedelta(0)))


def _pg_engine():
    return get_engine(resolve_pg_dsn())


def _pg_case_numbers_for_property(identifier: str) -> list[str]:
    if not identifier:
        return []
    with _pg_engine().connect() as conn:
        rows = conn.execute(
            sa_text("""
                SELECT DISTINCT case_number FROM (
                    SELECT f.case_number_raw AS case_number
                    FROM foreclosures f
                    WHERE f.case_number_raw = :identifier
                       OR f.strap = :identifier
                       OR f.folio = :identifier
                    UNION ALL
                    SELECT fh.case_number_raw AS case_number
                    FROM foreclosures_history fh
                    WHERE fh.case_number_raw = :identifier
                       OR fh.strap = :identifier
                       OR fh.folio = :identifier
                ) t
                WHERE case_number IS NOT NULL AND btrim(case_number) != ''
                ORDER BY case_number
            """),
            {"identifier": identifier},
        ).fetchall()
        return [str(r[0]) for r in rows if r and r[0]]


def _resolve_chain_folio(
    conn: Any,
    identifier: str | None,
    case_number: str | None = None,
) -> str | None:
    if case_number:
        row = conn.execute(
            sa_text("""
                SELECT folio
                FROM (
                    SELECT folio, auction_date
                    FROM foreclosures
                    WHERE case_number_raw = :case_number
                       OR case_number_norm = :case_number
                    UNION ALL
                    SELECT folio, auction_date
                    FROM foreclosures_history
                    WHERE case_number_raw = :case_number
                       OR case_number_norm = :case_number
                ) x
                WHERE folio IS NOT NULL AND btrim(folio) <> ''
                ORDER BY auction_date DESC NULLS LAST
                LIMIT 1
            """),
            {"case_number": case_number},
        ).fetchone()
        if row and row[0]:
            return str(row[0])

    if identifier:
        row = conn.execute(
            sa_text("""
                SELECT folio
                FROM (
                    SELECT folio, auction_date
                    FROM foreclosures
                    WHERE folio = :identifier
                       OR strap = :identifier
                       OR case_number_raw = :identifier
                       OR case_number_norm = :identifier
                    UNION ALL
                    SELECT folio, auction_date
                    FROM foreclosures_history
                    WHERE folio = :identifier
                       OR strap = :identifier
                       OR case_number_raw = :identifier
                       OR case_number_norm = :identifier
                ) x
                WHERE folio IS NOT NULL AND btrim(folio) <> ''
                ORDER BY auction_date DESC NULLS LAST
                LIMIT 1
            """),
            {"identifier": identifier},
        ).fetchone()
        if row and row[0]:
            return str(row[0])

        row = conn.execute(
            sa_text("""
                SELECT folio
                FROM hcpa_bulk_parcels
                WHERE strap = :identifier OR folio = :identifier
                LIMIT 1
            """),
            {"identifier": identifier},
        ).fetchone()
        if row and row[0]:
            return str(row[0])

        row = conn.execute(
            sa_text("""
                SELECT folio
                FROM hcpa_allsales
                WHERE folio = :identifier
                LIMIT 1
            """),
            {"identifier": identifier},
        ).fetchone()
        if row and row[0]:
            return str(row[0])

    return None


def _pg_chain_for_property(identifier: str, case_number: str | None = None) -> list[dict[str, Any]]:
    with _pg_engine().connect() as conn:
        folio = _resolve_chain_folio(conn, identifier, case_number)
        if not folio:
            return []

        rows = (
            conn.execute(
                sa_text("""
                SELECT
                    seq_no,
                    sale_date,
                    sale_type,
                    sale_amount,
                    grantor,
                    grantee,
                    or_book,
                    or_page,
                    doc_num,
                    days_since_prev,
                    link_score,
                    link_ok,
                    link_reason
                FROM fn_title_chain(:folio)
                ORDER BY seq_no
            """),
                {"folio": folio},
            )
            .mappings()
            .fetchall()
        )

        chain_rows: list[dict[str, Any]] = []
        for row in rows:
            reason = str(row.get("link_reason") or "")
            if reason == "NAME_MISMATCH":
                link_status = "BROKEN"
            elif reason == "MISSING_PARTY":
                link_status = "INCOMPLETE"
            elif reason == "FUZZY_MATCH":
                link_status = "FUZZY"
            elif reason == "ROOT_BOUNDARY":
                link_status = "IMPLIED"
            else:
                link_status = "LINKED"

            instrument = row.get("doc_num")
            if not instrument and row.get("or_book") and row.get("or_page"):
                instrument = f"{row['or_book']}/{row['or_page']}"

            chain_rows.append({
                "sequence_no": row.get("seq_no"),
                "acquisition_date": row.get("sale_date"),
                "acquisition_doc_type": row.get("sale_type"),
                "acquisition_price": row.get("sale_amount"),
                "acquired_from": row.get("grantor"),
                "owner_name": row.get("grantee"),
                "acquisition_instrument": instrument,
                "link_status": link_status,
                "link_score": row.get("link_score"),
                "link_reason": reason,
                "days_since_prev": row.get("days_since_prev"),
            })

        return chain_rows


def _pg_chain_gaps_for_property(
    identifier: str,
    case_number: str | None = None,
) -> list[dict[str, Any]]:
    with _pg_engine().connect() as conn:
        folio = _resolve_chain_folio(conn, identifier, case_number)
        if not folio:
            return []
        rows = (
            conn.execute(
                sa_text("""
                SELECT
                    gap_type,
                    seq_prev,
                    seq_next,
                    expected_from_party,
                    observed_to_party,
                    missing_from_date,
                    missing_to_date,
                    recommended_source,
                    detail
                FROM fn_title_chain_gaps(:folio)
            """),
                {"folio": folio},
            )
            .mappings()
            .fetchall()
        )
        return [dict(r) for r in rows]


def _pg_documents_for_property(identifier: str) -> list[dict[str, Any]]:
    project_root = Path(__file__).resolve().parents[3]
    foreclosure_root = (project_root / "data" / "Foreclosure").resolve()
    docs: list[dict[str, Any]] = []
    next_id = 1

    for case_num in _pg_case_numbers_for_property(identifier):
        doc_dir = foreclosure_root / case_num / "documents"
        if not doc_dir.is_dir():
            continue
        for pdf in sorted(doc_dir.glob("*.pdf")):
            try:
                rel_path = str(pdf.resolve().relative_to(project_root.resolve()))
            except Exception:
                logger.exception(f"Failed to resolve relative PDF path for case={case_num} file={pdf}")
                rel_path = str(pdf.resolve())
            docs.append({
                "id": next_id,
                "folio": identifier,
                "case_number": case_num,
                "document_type": ("FINAL_JUDGMENT" if "judgment" in pdf.name.lower() else "PDF"),
                "file_path": rel_path,
                "recording_date": None,
                "instrument_number": None,
                "party1": None,
                "party2": None,
            })
            next_id += 1
    return docs


def _coerce_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def _coerce_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return parsed
    return []


def _to_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_source_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    return text or None


def _extract_instrument_tokens(value: Any) -> list[str]:
    text = str(value or "")
    if not text:
        return []
    return re.findall(r"\d{8,}", text)


def _instrument_search_url(instrument_value: Any) -> str | None:
    tokens = _extract_instrument_tokens(instrument_value)
    instrument = tokens[0] if tokens else str(instrument_value or "").strip()
    if not instrument:
        return None
    return f"https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=320&OBKey__1006_1={quote(instrument)}"


def _build_document_token_index(docs: list[dict[str, Any]]) -> dict[str, int]:
    index: dict[str, int] = {}
    for doc in docs:
        doc_id = doc.get("id")
        if not isinstance(doc_id, int):
            continue
        for token in _extract_instrument_tokens(doc.get("instrument_number")):
            index.setdefault(token, doc_id)
        file_path = str(doc.get("file_path") or "")
        if file_path:
            stem = Path(file_path).stem
            for token in re.findall(r"\d{8,}", stem):
                index.setdefault(token, doc_id)
    return index


def _doc_mtime_iso(file_path: str | None) -> str | None:
    if not file_path:
        return None
    project_root = Path(__file__).resolve().parents[3]
    abs_path = (project_root / file_path).resolve()
    try:
        if abs_path.is_file():
            return datetime.fromtimestamp(abs_path.stat().st_mtime, tz=UTC_TZ).isoformat(timespec="seconds")
    except Exception:
        logger.exception(f"Failed to read document mtime for {file_path!r}")
    return None


def _build_property_sources(
    *,
    identifier: str,
    auction: dict[str, Any],
    market_row: dict[str, Any] | None,
    docs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    def _add(
        source_name: str,
        url: str | None,
        description: str,
        created_at: Any = None,
    ) -> None:
        if not url:
            return
        key = (source_name, url)
        if key in seen:
            return
        seen.add(key)
        rows.append({
            "source_name": source_name,
            "url": url,
            "description": description,
            "created_at": _format_source_timestamp(created_at) or "-",
        })

    if market_row:
        updated_at = market_row.get("updated_at")
        detail_url = str(market_row.get("detail_url") or "").strip()
        redfin_json = _coerce_json_dict(market_row.get("redfin_json"))
        zillow_json = _coerce_json_dict(market_row.get("zillow_json"))
        hh_json = _coerce_json_dict(market_row.get("homeharvest_json"))

        redfin_url = str(redfin_json.get("detail_url") or "").strip()
        if not redfin_url and "redfin.com" in detail_url:
            redfin_url = detail_url
        _add(
            "Redfin",
            redfin_url,
            "Market snapshot from Redfin scraper",
            updated_at,
        )

        zillow_url = str(zillow_json.get("detail_url") or "").strip()
        _add(
            "Zillow",
            zillow_url,
            "Market snapshot from Zillow scraper",
            updated_at,
        )

        hh_url = str(hh_json.get("property_url") or hh_json.get("detail_url") or "").strip()
        if not hh_url and "realtor.com" in detail_url:
            hh_url = detail_url
        _add(
            "HomeHarvest",
            hh_url,
            "Market snapshot from HomeHarvest feed",
            updated_at,
        )

    for doc in docs:
        if doc.get("document_type") != "FINAL_JUDGMENT":
            continue
        doc_id = doc.get("id")
        if not isinstance(doc_id, int):
            continue
        _add(
            "Final Judgment PDF",
            f"/property/{quote(identifier)}/doc/{doc_id}",
            f"Downloaded judgment document for case {doc.get('case_number') or ''}".strip(),
            _doc_mtime_iso(doc.get("file_path")),
        )

    _add(
        "ORI Search",
        "https://publicaccess.hillsclerk.com/OfficialRecords",
        "Official Records search was executed for this property",
        auction.get("step_ori_searched"),
    )
    _add(
        "Judgment Extraction",
        "https://publicrecords.hillsclerk.com/",
        "Judgment extraction stage completed",
        auction.get("step_judgment_extracted"),
    )
    return rows


def _market_photo_urls(
    identifier: str,
    local_paths: list[Any],
    cdn_urls: list[Any],
) -> tuple[list[str], list[dict[str, str | None]]]:
    local_files: list[str] = []
    for raw in local_paths:
        path_str = str(raw or "").strip()
        if not path_str:
            continue
        filename = Path(path_str).name
        if not filename:
            continue
        local_files.append(f"/property/{quote(identifier)}/photos/{quote(filename)}")

    cdn_list = [str(url).strip() for url in cdn_urls if str(url or "").strip()]
    photos_with_fallback: list[dict[str, str | None]] = []
    max_len = max(len(local_files), len(cdn_list))
    for i in range(max_len):
        local_url = local_files[i] if i < len(local_files) else None
        cdn_url = cdn_list[i] if i < len(cdn_list) else None
        if local_url:
            photos_with_fallback.append({"url": local_url, "cdn_fallback": cdn_url})
        elif cdn_url:
            photos_with_fallback.append({"url": cdn_url, "cdn_fallback": None})

    photos = [str(p["url"]) for p in photos_with_fallback if p.get("url")]
    return photos, photos_with_fallback


def _pg_tax_liens_for_property(
    conn: Any,
    *,
    strap: str | None,
    folio: str | None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    where_clauses: list[str] = []
    params: dict[str, Any] = {"lim": max(1, min(limit, 1000))}
    if strap:
        where_clauses.append("oe.strap = :strap")
        params["strap"] = strap
    elif folio:
        where_clauses.append("oe.folio = :folio")
        params["folio"] = folio
    if not where_clauses:
        return []

    rows = (
        conn.execute(
            sa_text(f"""
            SELECT
                oe.id,
                oe.recording_date,
                oe.encumbrance_type::text AS encumbrance_type,
                oe.raw_document_type,
                oe.amount,
                oe.survival_status,
                oe.party1,
                oe.party2,
                oe.instrument_number,
                CASE
                    WHEN oe.encumbrance_type::text = 'mortgage'
                        THEN COALESCE(NULLIF(oe.party2, ''), NULLIF(oe.party1, ''), '')
                    ELSE COALESCE(NULLIF(oe.party1, ''), NULLIF(oe.party2, ''), '')
                END AS creditor
            FROM ori_encumbrances oe
            WHERE ({" OR ".join(where_clauses)})
              AND (
                    UPPER(COALESCE(oe.raw_document_type, '')) LIKE '%LNCORPTX%'
                    OR UPPER(COALESCE(oe.raw_document_type, '')) LIKE '%TAX LIEN%'
                    OR UPPER(COALESCE(oe.raw_document_type, '')) LIKE '%(TL)%'
                    OR UPPER(COALESCE(oe.party1, '')) LIKE '%INTERNAL REVENUE%'
                    OR UPPER(COALESCE(oe.party1, '')) LIKE '% IRS %'
                    OR UPPER(COALESCE(oe.party2, '')) LIKE '%INTERNAL REVENUE%'
                    OR UPPER(COALESCE(oe.party2, '')) LIKE '% IRS %'
                    OR UPPER(COALESCE(oe.party1, '')) LIKE '%TAX COLLECTOR%'
                    OR UPPER(COALESCE(oe.party2, '')) LIKE '%TAX COLLECTOR%'
              )
              AND UPPER(COALESCE(oe.raw_document_type, '')) NOT LIKE '%MORTGAGE%'
              AND UPPER(COALESCE(oe.raw_document_type, '')) NOT LIKE '%ASSIGNMENT/TAXES%'
            ORDER BY oe.recording_date DESC NULLS LAST, oe.id DESC
            LIMIT :lim
        """),
            params,
        )
        .mappings()
        .fetchall()
    )
    return [dict(r) for r in rows]


def _pg_nocs_for_property(
    conn: Any,
    *,
    strap: str | None,
    folio: str | None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    where_clauses: list[str] = []
    params: dict[str, Any] = {"lim": max(1, min(limit, 1000))}
    if strap:
        where_clauses.append("oe.strap = :strap")
        params["strap"] = strap
    elif folio:
        where_clauses.append("oe.folio = :folio")
        params["folio"] = folio
    if not where_clauses:
        return []

    rows = (
        conn.execute(
            sa_text(f"""
            SELECT
                oe.id AS encumbrance_id,
                oe.recording_date,
                oe.instrument_number,
                oe.party1,
                oe.party2,
                oe.legal_description,
                oe.raw_document_type
            FROM ori_encumbrances oe
            WHERE ({" OR ".join(where_clauses)})
              AND (
                    UPPER(COALESCE(oe.raw_document_type, '')) LIKE '%(NOC)%'
                    OR UPPER(COALESCE(oe.raw_document_type, '')) LIKE '%NOTICE OF COMMENCEMENT%'
                    OR UPPER(COALESCE(oe.raw_document_type, '')) LIKE 'NOC%'
              )
            ORDER BY oe.recording_date DESC NULLS LAST, oe.id DESC
            LIMIT :lim
        """),
            params,
        )
        .mappings()
        .fetchall()
    )
    nocs = []
    for row in rows:
        item = dict(row)
        item["id"] = None
        item["instrument_url"] = _instrument_search_url(item.get("instrument_number"))
        nocs.append(item)
    return nocs


def _match_nocs_to_permits(
    nocs: list[dict[str, Any]],
    permits: list[dict[str, Any]],
) -> None:
    """Link each NOC to its closest permit by date proximity.

    Real-world flow: NOC filed -> permit pulled -> work done.
    A permit is considered a match if its issue_date falls between
    30 days before and 730 days after the NOC recording_date.
    Mutates NOC dicts in-place.
    """
    if not nocs or not permits:
        return

    for noc in nocs:
        rec_raw = noc.get("recording_date")
        if not rec_raw:
            continue
        try:
            rec_date = rec_raw if isinstance(rec_raw, date) else datetime.strptime(str(rec_raw)[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            continue

        best_permit = None
        best_gap = None
        for permit in permits:
            issue_raw = permit.get("issue_date")
            if not issue_raw:
                continue
            try:
                issue_date = (
                    issue_raw if isinstance(issue_raw, date) else datetime.strptime(str(issue_raw)[:10], "%Y-%m-%d").date()
                )
            except (ValueError, TypeError):
                continue

            delta_days = (issue_date - rec_date).days
            # Permit issued between 30 days before and 730 days after the NOC
            if -30 <= delta_days <= 730:
                gap = abs(delta_days)
                if best_gap is None or gap < best_gap:
                    best_gap = gap
                    best_permit = permit

        if best_permit:
            noc["matched_permit"] = best_permit.get("permit_number")
            noc["matched_permit_date"] = best_permit.get("issue_date")


def _pg_flr_liens_for_property(conn: Any, *, owner_name: str | None) -> list[dict[str, Any]]:
    """Query Sunbiz Federal Lien Registry (FLR) for UCC/federal tax liens by owner name."""
    if not owner_name or len(owner_name.strip()) < 3:
        return []
    rows = (
        conn.execute(
            sa_text("""
            SELECT doc_number, filing_date, filing_type,
                   secured_party, debtor_name, similarity_score
            FROM get_ucc_exposure(:name)
            ORDER BY similarity_score DESC, filing_date DESC
        """),
            {"name": owner_name.strip()},
        )
        .mappings()
        .fetchall()
    )
    return [dict(r) for r in rows]


def _pg_tax_status_for_property(
    *,
    strap: str | None,
    folio: str | None,
    identifier: str,
    owner_name: str | None = None,
) -> dict[str, Any]:
    with _pg_engine().connect() as conn:
        id_values = [v for v in (folio, strap, identifier) if v]
        if not id_values:
            return {
                "has_tax_liens": False,
                "tax_status": None,
                "tax_warrant": False,
                "total_amount_due": None,
                "current_year": None,
                "history": [],
                "liens": [],
                "flr_liens": [],
            }

        # Query A: current year full detail
        row = (
            conn.execute(
                sa_text("""
                SELECT tax_year, homestead_exempt, estimated_annual_tax,
                       just_value, just_value_homestead,
                       assessed_value_school, assessed_value_nonschool,
                       assessed_value_homestead,
                       taxable_value_school, taxable_value_nonschool,
                       homestead_exempt_value, widow_exempt, widow_exempt_value,
                       disability_exempt, disability_exempt_value,
                       veteran_exempt, veteran_exempt_value,
                       ag_exempt, ag_exempt_value,
                       soh_differential, total_millage, county_millage,
                       school_millage, city_millage, property_use_code
                FROM dor_nal_parcels
                WHERE folio = ANY(:ids)
                   OR strap = ANY(:ids)
                   OR parcel_id = ANY(:ids)
                ORDER BY tax_year DESC
                LIMIT 1
            """),
                {"ids": id_values},
            )
            .mappings()
            .fetchone()
        )
        current_year = dict(row) if row else None

        # Query B: all years for history table
        history_rows = (
            conn.execute(
                sa_text("""
                SELECT DISTINCT ON (tax_year)
                       tax_year, just_value,
                       GREATEST(assessed_value_school, assessed_value_nonschool)
                           AS assessed_value,
                       GREATEST(taxable_value_school, taxable_value_nonschool)
                           AS taxable_value,
                       estimated_annual_tax, homestead_exempt, total_millage
                FROM dor_nal_parcels
                WHERE folio = ANY(:ids)
                   OR strap = ANY(:ids)
                   OR parcel_id = ANY(:ids)
                ORDER BY tax_year DESC, id DESC
            """),
                {"ids": id_values},
            )
            .mappings()
            .fetchall()
        )
        history = _compute_yoy([dict(r) for r in history_rows])

        liens = _pg_tax_liens_for_property(conn, strap=strap, folio=folio)
        flr_liens = _pg_flr_liens_for_property(conn, owner_name=owner_name)
        liens_total = sum(_as_float(item.get("amount")) for item in liens)
        amount = row.get("estimated_annual_tax") if row else None
        tax_warrant = any("WARRANT" in str(item.get("raw_document_type") or "").upper() for item in liens)
        return {
            "has_tax_liens": bool(liens) or bool((amount or 0) > 0),
            "tax_status": f"Tax Year {row.get('tax_year')}" if row else None,
            "tax_warrant": tax_warrant,
            "total_amount_due": liens_total if liens_total > 0 else _to_optional_float(amount),
            "current_year": current_year,
            "history": history,
            "liens": liens,
            "flr_liens": flr_liens,
        }


def _pg_permits_for_property(foreclosure_id: int) -> list[dict[str, Any]]:
    with _pg_engine().connect() as conn:
        rows = (
            conn.execute(
                sa_text("""
                SELECT
                    event_date AS issue_date,
                    instrument_number AS permit_number,
                    event_subtype AS permit_type,
                    description,
                    amount AS estimated_cost,
                    CASE
                        WHEN description ~* '(closed|complete|final|expired)'
                            THEN 'Closed'
                        ELSE 'Open'
                    END AS status
                FROM foreclosure_title_events
                WHERE foreclosure_id = :fid
                  AND event_source IN ('COUNTY_PERMIT', 'TAMPA_PERMIT')
                ORDER BY event_date DESC
            """),
                {"fid": foreclosure_id},
            )
            .mappings()
            .fetchall()
        )
        return [dict(r) for r in rows]


def _as_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _pct_change(old: Any, new: Any) -> float | None:
    """Return percentage change from old to new, or None if either is null/zero."""
    try:
        o, n = float(old), float(new)
    except (TypeError, ValueError):
        return None
    if o == 0:
        return None
    return ((n - o) / o) * 100


def _compute_yoy(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Annotate history rows (sorted tax_year DESC) with YoY % change fields."""
    for i, row in enumerate(rows):
        prev = rows[i + 1] if i + 1 < len(rows) else None
        if prev:
            row["yoy_just_value_pct"] = _pct_change(prev.get("just_value"), row.get("just_value"))
            row["yoy_taxable_value_pct"] = _pct_change(prev.get("taxable_value"), row.get("taxable_value"))
            row["yoy_tax_pct"] = _pct_change(prev.get("estimated_annual_tax"), row.get("estimated_annual_tax"))
        else:
            row["yoy_just_value_pct"] = None
            row["yoy_taxable_value_pct"] = None
            row["yoy_tax_pct"] = None
    return rows


def _normalized_survival_status(value: Any) -> str:
    return str(value or "").strip().upper()


def _pg_encumbrances_for_property(
    conn: Any,
    *,
    strap: str | None,
    folio: str | None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    if not strap and not folio:
        return []
    where_clauses: list[str] = []
    params: dict[str, Any] = {"lim": max(1, min(limit, 2000))}
    if strap:
        where_clauses.append("oe.strap = :strap")
        params["strap"] = strap
    elif folio:
        where_clauses.append("oe.folio = :folio")
        params["folio"] = folio
    if not where_clauses:
        return []

    rows = (
        conn.execute(
            sa_text(f"""
            SELECT
                oe.id,
                oe.recording_date,
                oe.encumbrance_type::text AS encumbrance_type,
                oe.amount,
                oe.amount_confidence,
                oe.survival_status,
                oe.survival_reason,
                COALESCE(oe.is_satisfied, FALSE) AS is_satisfied,
                oe.instrument_number AS instrument,
                oe.instrument_number,
                oe.book,
                oe.page,
                oe.case_number,
                oe.party1,
                oe.party2,
                oe.raw_document_type,
                CASE
                    WHEN oe.encumbrance_type::text = 'mortgage'
                        THEN COALESCE(NULLIF(oe.party2, ''), NULLIF(oe.party1, ''), '')
                    ELSE COALESCE(NULLIF(oe.party1, ''), NULLIF(oe.party2, ''), '')
                END AS creditor
            FROM ori_encumbrances oe
            WHERE ({" OR ".join(where_clauses)})
              AND oe.encumbrance_type::text != 'noc'
            ORDER BY oe.recording_date DESC NULLS LAST, oe.id DESC
            LIMIT :lim
        """),
            params,
        )
        .mappings()
        .fetchall()
    )

    encumbrances: list[dict[str, Any]] = []
    for row in rows:
        encumbrance = dict(row)
        reason_upper = str(encumbrance.get("survival_reason") or "").upper()
        encumbrance["is_joined"] = "JOINED AS DEFENDANT" in reason_upper
        encumbrance["is_inferred"] = "INFER" in reason_upper
        encumbrances.append(encumbrance)
    return encumbrances


def _summarize_encumbrances(encumbrances: list[dict[str, Any]]) -> dict[str, Any]:
    # Treat UNCERTAIN as risk-bearing until proven extinguished.
    risk_statuses = {"SURVIVED", "UNCERTAIN"}
    liens_total = 0
    liens_survived = 0
    liens_uncertain = 0
    liens_surviving = 0
    liens_total_amount = 0.0
    surviving_unknown_amount = 0

    for enc in encumbrances:
        if bool(enc.get("is_satisfied")):
            continue
        liens_total += 1
        status = _normalized_survival_status(enc.get("survival_status"))
        if status == "SURVIVED":
            liens_survived += 1
        elif status == "UNCERTAIN":
            liens_uncertain += 1
        if status in risk_statuses:
            amount = enc.get("amount")
            if amount is None:
                surviving_unknown_amount += 1
            else:
                liens_total_amount += _as_float(amount)
    liens_surviving = liens_survived + liens_uncertain

    return {
        "liens_total": liens_total,
        "liens_survived": liens_survived,
        "liens_uncertain": liens_uncertain,
        "liens_surviving": liens_surviving,
        "liens_total_amount": liens_total_amount,
        "surviving_unknown_amount": surviving_unknown_amount,
    }


def _pg_market_snapshot(
    conn: Any,
    *,
    identifier: str,
    auction: dict[str, Any],
    parcel: dict[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    market_row = None
    strap = auction.get("strap")
    folio = auction.get("folio")
    if strap:
        market_row = (
            conn.execute(
                sa_text("""
                SELECT *
                FROM property_market
                WHERE strap = :strap
                LIMIT 1
            """),
                {"strap": strap},
            )
            .mappings()
            .fetchone()
        )
    if not market_row and folio:
        market_row = (
            conn.execute(
                sa_text("""
                SELECT *
                FROM property_market
                WHERE folio = :folio
                ORDER BY updated_at DESC NULLS LAST
                LIMIT 1
            """),
                {"folio": folio},
            )
            .mappings()
            .fetchone()
        )

    row = dict(market_row) if market_row else None
    redfin_json = _coerce_json_dict(row.get("redfin_json")) if row else {}
    zillow_json = _coerce_json_dict(row.get("zillow_json")) if row else {}
    hh_json = _coerce_json_dict(row.get("homeharvest_json")) if row else {}

    estimates = {
        "zillow_zestimate": _to_optional_float(zillow_json.get("zestimate")),
        "homeharvest_estimated_value": _to_optional_float(hh_json.get("estimated_value")),
        "redfin_estimate": _to_optional_float(redfin_json.get("zestimate") or redfin_json.get("redfin_estimate")),
        "realtor_estimate": None,
    }
    estimate_values = [v for v in estimates.values() if v is not None]

    list_prices = {
        "redfin_list_price": _to_optional_float(redfin_json.get("list_price") or (row or {}).get("list_price")),
        "realtor_list_price": None,
        "homeharvest_list_price": _to_optional_float(hh_json.get("list_price")),
    }

    fallback_market_value = (
        _to_optional_float((row or {}).get("zestimate"))
        or _to_optional_float((row or {}).get("list_price"))
        or _to_optional_float(auction.get("market_value"))
        or _to_optional_float((parcel or {}).get("market_value"))
        or _to_optional_float(auction.get("assessed_value"))
        or 0.0
    )
    blended_estimate = round(sum(estimate_values) / len(estimate_values), 2) if estimate_values else fallback_market_value

    local_paths = _coerce_json_list((row or {}).get("photo_local_paths"))
    cdn_urls = _coerce_json_list((row or {}).get("photo_cdn_urls"))
    photos, photos_with_fallback = _market_photo_urls(
        identifier,
        local_paths=local_paths,
        cdn_urls=cdn_urls,
    )

    return (
        {
            "blended_estimate": blended_estimate,
            "estimates": estimates,
            "list_prices": list_prices,
            "photos": photos,
            "photos_with_fallback": photos_with_fallback,
            "primary_source": (row or {}).get("primary_source"),
            "updated_at": (row or {}).get("updated_at"),
        },
        row,
    )


def _pg_property_detail(identifier: str) -> dict[str, Any] | None:
    with _pg_engine().connect() as conn:
        row = (
            conn.execute(
                sa_text("""
                SELECT *
                FROM foreclosures
                WHERE case_number_raw = :identifier
                   OR strap = :identifier
                   OR folio = :identifier
                ORDER BY auction_date DESC, updated_at DESC NULLS LAST
                LIMIT 1
            """),
                {"identifier": identifier},
            )
            .mappings()
            .fetchone()
        )
        if not row:
            row = (
                conn.execute(
                    sa_text("""
                    SELECT *
                    FROM foreclosures_history
                    WHERE case_number_raw = :identifier
                       OR strap = :identifier
                       OR folio = :identifier
                    ORDER BY auction_date DESC, updated_at DESC NULLS LAST
                    LIMIT 1
                """),
                    {"identifier": identifier},
                )
                .mappings()
                .fetchone()
            )
        if not row:
            return None

        auction = dict(row)
        case_number = auction.get("case_number_raw")
        strap_or_folio = auction.get("strap") or auction.get("folio") or identifier

        parcel = None
        parcel_clauses: list[str] = []
        parcel_params: dict[str, Any] = {}
        if auction.get("strap"):
            parcel_clauses.append("strap = :strap")
            parcel_params["strap"] = auction.get("strap")
        if auction.get("folio"):
            parcel_clauses.append("folio = :folio")
            parcel_params["folio"] = auction.get("folio")
        if parcel_clauses:
            parcel = (
                conn.execute(
                    sa_text(f"""
                    SELECT *
                    FROM hcpa_bulk_parcels
                    WHERE {" OR ".join(parcel_clauses)}
                    ORDER BY source_file_id DESC NULLS LAST
                    LIMIT 1
                """),
                    parcel_params,
                )
                .mappings()
                .fetchone()
            )
        parcel_dict = dict(parcel) if parcel else {}

        judgment_data = auction.get("judgment_data")
        if isinstance(judgment_data, str):
            try:
                judgment_data = json.loads(judgment_data)
            except json.JSONDecodeError:
                judgment_data = None
        if not isinstance(judgment_data, dict):
            judgment_data = None

        judgment_map = judgment_data or {}
        plaintiff = str(judgment_map.get("plaintiff") or "").strip() or None
        foreclosure_type = str(judgment_map.get("foreclosure_type") or "").strip() or None
        lis_pendens_block = judgment_map.get("lis_pendens")
        lis_pendens_date = None
        if isinstance(lis_pendens_block, dict):
            lis_pendens_date = lis_pendens_block.get("recording_date") or lis_pendens_block.get("date")
        if not lis_pendens_date:
            lis_pendens_date = judgment_map.get("lis_pendens_date")

        # Fallback: pull LP recording_date from ori_encumbrances when
        # the judgment PDF didn't contain an explicit date.
        if not lis_pendens_date and auction.get("strap") and plaintiff:
            lp_row = (
                conn.execute(
                    sa_text("""
                    SELECT recording_date
                    FROM ori_encumbrances
                    WHERE strap = :strap
                      AND encumbrance_type = 'lis_pendens'
                      AND recording_date IS NOT NULL
                      AND UPPER(party1) LIKE '%' || :plaintiff_prefix || '%'
                    ORDER BY recording_date ASC
                    LIMIT 1
                """),
                    {
                        "strap": auction["strap"],
                        "plaintiff_prefix": plaintiff.split(",")[0].strip().upper(),
                    },
                )
                .mappings()
                .fetchone()
            )
            if lp_row:
                lis_pendens_date = str(lp_row["recording_date"])

        defendant = None
        defendants = judgment_map.get("defendants")
        if isinstance(defendants, list):
            names = [str(v).strip() for v in defendants if str(v).strip()]
            if names:
                defendant = ", ".join(names[:3])
        elif isinstance(defendants, str):
            defendant = defendants.strip() or None
        if not defendant:
            defendant = str(judgment_map.get("defendant") or "").strip() or None

        plaintiff_max_bid = _to_optional_float(
            judgment_map.get("plaintiff_max_bid") or judgment_map.get("plaintiff_maximum_bid") or judgment_map.get("max_bid")
        )

        market, market_row = _pg_market_snapshot(
            conn,
            identifier=strap_or_folio,
            auction=auction,
            parcel=parcel_dict,
        )
        market_value = (
            market.get("blended_estimate")
            or auction.get("market_value")
            or parcel_dict.get("market_value")
            or auction.get("assessed_value")
            or 0
        )

        raw_foreclosure_id = auction.get("foreclosure_id")
        try:
            foreclosure_id = int(raw_foreclosure_id) if raw_foreclosure_id is not None else 0
        except (TypeError, ValueError):
            logger.warning(
                f"Invalid foreclosure_id for property detail identifier={identifier!r} raw_value={raw_foreclosure_id!r}"
            )
            foreclosure_id = 0
        documents = _pg_documents_for_property(strap_or_folio)
        document_tokens = _build_document_token_index(documents)
        permits = _pg_permits_for_property(foreclosure_id)
        nocs = _pg_nocs_for_property(
            conn,
            strap=auction.get("strap"),
            folio=auction.get("folio"),
        )
        for noc in nocs:
            for token in _extract_instrument_tokens(noc.get("instrument_number")):
                doc_id = document_tokens.get(token)
                if doc_id is not None:
                    noc["id"] = doc_id
                    break
        _match_nocs_to_permits(nocs, permits)

        encumbrances = _pg_encumbrances_for_property(
            conn,
            strap=auction.get("strap"),
            folio=auction.get("folio"),
        )
        enc_summary = _summarize_encumbrances(encumbrances)
        est_surviving_debt = _as_float(enc_summary["liens_total_amount"])
        net_equity = _as_float(market_value) - _as_float(auction.get("final_judgment_amount")) - est_surviving_debt

        cdd_warning = get_pg_queries().get_cdd_warning(strap_or_folio)

        enrichments = {
            "permits_total": len(permits),
            "permits_open": sum(1 for p in permits if str(p.get("status") or "").lower() in {"open", "active", "issued"}),
            "liens_survived": int(enc_summary["liens_survived"]),
            "liens_uncertain": int(enc_summary["liens_uncertain"]),
            "liens_surviving": int(enc_summary["liens_surviving"]),
            "liens_total_amount": est_surviving_debt,
            "liens_total": int(enc_summary["liens_total"]),
            "flood_zone": None,
            "flood_risk": None,
            "insurance_required": False,
            "cdd_warning": cdd_warning,
            "has_enrichments": bool(len(permits) > 0 or enc_summary["liens_total"] > 0 or cdd_warning),
        }

        auction_payload = {
            "case_number": case_number,
            "auction_type": str(auction.get("auction_type") or "foreclosure").upper(),
            "auction_date": auction.get("auction_date"),
            "property_address": auction.get("property_address") or parcel_dict.get("property_address"),
            "assessed_value": auction.get("assessed_value"),
            "final_judgment_amount": auction.get("final_judgment_amount"),
            "opening_bid": auction.get("winning_bid"),
            "status": auction.get("auction_status"),
            "owner_name": auction.get("owner_name") or parcel_dict.get("owner_name"),
            "plaintiff_max_bid": plaintiff_max_bid,
            "plaintiff": plaintiff,
            "foreclosure_type": foreclosure_type,
            "lis_pendens_date": lis_pendens_date,
            "defendant": defendant,
            "extracted_judgment_data": judgment_data,
            "judgment_extracted_at": auction.get("step_judgment_extracted"),
            "has_valid_parcel_id": bool(auction.get("strap") or auction.get("folio")),
            "folio": strap_or_folio,
            "est_surviving_debt": est_surviving_debt,
        }

        return {
            "folio": strap_or_folio,
            "auction": auction_payload,
            "parcel": parcel_dict,
            "parcels_data": parcel_dict,
            "encumbrances": encumbrances,
            "chain": _pg_chain_for_property(strap_or_folio, case_number),
            "nocs": nocs,
            "sales": get_pg_queries().get_sales_history(strap_or_folio),
            "net_equity": net_equity,
            "market_value": market_value,
            "est_surviving_debt": est_surviving_debt,
            "is_toxic_title": bool((auction.get("unsatisfied_encumbrance_count") or 0) > 2 or enc_summary["liens_surviving"] > 0),
            "market": market,
            "enrichments": enrichments,
            "sources": _build_property_sources(
                identifier=strap_or_folio,
                auction=auction,
                market_row=market_row,
                docs=documents,
            ),
            "_foreclosure_id": foreclosure_id,
            "_strap": auction.get("strap"),
            "_folio_raw": auction.get("folio"),
            "_case_number_raw": case_number,
        }


@router.get("/{folio}", response_class=HTMLResponse)
async def property_detail(request: Request, folio: str):
    """
    Full property detail page.
    """
    prop = _pg_property_detail(folio)

    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    # Enrich with PG data (graceful degradation)
    pg = get_pg_queries()
    pg_data = {}
    real_folio = prop.get("folio") or folio
    if pg.available:
        pg_data["subdivision"] = pg.get_subdivision_info(real_folio)
        pg_data["multi_unit"] = pg.is_multi_unit(real_folio)
        pg_data["pg_available"] = True
    else:
        pg_data["subdivision"] = None
        pg_data["multi_unit"] = None
        pg_data["pg_available"] = False

    return templates.TemplateResponse(
        "property.html",
        {
            "request": request,
            "property": prop,
            "auction": prop.get("auction", {}),
            "parcel": prop.get("parcel") or prop.get("parcels_data") or {},
            "encumbrances": prop.get("encumbrances", []),
            "net_equity": prop.get("net_equity", 0),
            "market_value": prop.get("market_value", 0),
            "market": prop.get("market", {}),
            "enrichments": prop.get("enrichments", {}),
            "pg_data": pg_data,
        },
    )


@router.get("/{folio}/liens", response_class=HTMLResponse)
async def property_liens(request: Request, folio: str):
    """
    HTMX partial - liens table for a property.
    """
    prop = _pg_property_detail(folio)

    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    encumbrances = prop.get("encumbrances", [])
    real_folio = prop.get("folio") or folio

    return templates.TemplateResponse(
        "partials/lien_table.html",
        {"request": request, "encumbrances": encumbrances, "auction": prop.get("auction", {}), "folio": real_folio},
    )


@router.get("/{folio}/documents", response_class=HTMLResponse)
async def property_documents(request: Request, folio: str):
    """
    HTMX partial - documents list for a property.
    """
    documents = _pg_documents_for_property(folio)

    return templates.TemplateResponse("partials/documents.html", {"request": request, "documents": documents, "folio": folio})


@router.get("/{folio}/analysis", response_class=HTMLResponse)
async def property_analysis(request: Request, folio: str):
    """
    HTMX partial - equity analysis card.
    """
    prop = _pg_property_detail(folio)

    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    return templates.TemplateResponse(
        "partials/analysis_card.html",
        {
            "request": request,
            "property": prop,
            "net_equity": prop.get("net_equity", 0),
            "market_value": prop.get("market_value", 0),
        },
    )


@router.get("/{folio}/sales", response_class=HTMLResponse)
async def property_sales_history(request: Request, folio: str):
    """
    HTMX partial - sales history for a property (PG-only).
    """
    pg = get_pg_queries()
    sales = pg.get_sales_history(folio) if pg.available else []
    return templates.TemplateResponse(
        "partials/pg_sales_history.html",
        {
            "request": request,
            "sales": sales,
            "folio": folio,
            "source": "pg" if pg.available else "pg_unavailable",
        },
    )


@router.get("/{folio}/market", response_class=HTMLResponse)
async def property_market(request: Request, folio: str):
    """
    HTMX partial - blended market data + HomeHarvest gallery.
    """
    prop = _pg_property_detail(folio)
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")
    market = prop.get("market") or {}
    return templates.TemplateResponse(
        "partials/market.html",
        {
            "request": request,
            "folio": prop.get("folio") or folio,
            "auction": prop.get("auction", {}),
            "market": market,
        },
    )


@router.get("/{folio}/tax", response_class=HTMLResponse)
async def property_tax(request: Request, folio: str):
    """
    HTMX partial - tax status and tax liens.
    """
    prop = _pg_property_detail(folio)
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")
    status = _pg_tax_status_for_property(
        strap=prop.get("_strap"),
        folio=prop.get("_folio_raw"),
        identifier=prop.get("folio") or folio,
        owner_name=(prop.get("auction") or {}).get("owner_name"),
    )
    return templates.TemplateResponse(
        "partials/tax.html",
        {
            "request": request,
            "folio": prop.get("folio") or folio,
            "auction": prop.get("auction", {}),
            "parcel": prop.get("parcel") or prop.get("parcels_data") or {},
            "tax": status,
        },
    )


def _parse_owner_name(owner_name: str) -> tuple[str, str]:
    """Parse owner name into (last_name, first_name) components.

    Handles common formats:
    - "LAST, FIRST MIDDLE" -> ("LAST", "FIRST")
    - "FIRST LAST" -> ("LAST", "FIRST")
    - "LAST NAME ONLY" -> ("LAST NAME ONLY", "")
    """
    if not owner_name:
        return ("", "")
    name = owner_name.strip().upper()
    if "," in name:
        parts = name.split(",", 1)
        last = parts[0].strip()
        first = parts[1].strip().split()[0] if parts[1].strip() else ""
        return (last, first)
    parts = name.split()
    if len(parts) >= 2:
        return (parts[-1], parts[0])
    return (name, "")


def _pg_personal_dossier(
    owner_name: str,
    strap: str | None,
    folio: str | None,
) -> dict[str, Any]:
    """Build a personal dossier for the given owner from PG public records.

    Queries clerk civil cases, garnishments, criminal records, UCC filings,
    and cross-property ORI documents using fuzzy name matching.
    Returns a dict with keys: identity, civil_cases, garnishments,
    criminal_cases, ucc_liens, ori_docs.
    """
    result: dict[str, Any] = {
        "identity": {"aliases": [], "addresses": []},
        "civil_cases": [],
        "garnishments": [],
        "criminal_cases": [],
        "ucc_liens": [],
        "ori_docs": [],
    }
    if not owner_name or not owner_name.strip():
        return result

    last_name, first_name = _parse_owner_name(owner_name)
    owner_upper = owner_name.strip().upper()

    if not last_name:
        return result

    with _pg_engine().connect() as conn:
        # ── 1. Civil cases via clerk_civil_parties (trgm indexes on last_name) ──
        try:
            civil_rows = (
                conn.execute(
                    sa_text("""
                        SELECT DISTINCT ON (p.case_number)
                            p.case_number,
                            cc.case_type,
                            cc.case_status,
                            cc.filing_date,
                            p.party_type,
                            COALESCE(cc.style, p.business_name) AS style,
                            p.akas,
                            p.address1, p.address2, p.city, p.state,
                            p.zip AS zip_code,
                            similarity(p.last_name, :last_name) AS match_score
                        FROM clerk_civil_parties p
                        JOIN clerk_civil_cases cc ON cc.case_number = p.case_number
                        WHERE p.last_name % :last_name
                          AND similarity(p.last_name, :last_name) > 0.4
                          AND (
                              :first_name = ''
                              OR p.first_name IS NULL
                              OR similarity(p.first_name, :first_name) > 0.3
                          )
                        ORDER BY p.case_number, match_score DESC
                        LIMIT 100
                    """),
                    {"last_name": last_name, "first_name": first_name},
                )
                .mappings()
                .all()
            )
            aliases: set[str] = set()
            addresses: set[str] = set()
            for row in civil_rows:
                r = dict(row)
                result["civil_cases"].append(r)
                # Collect aliases
                if r.get("akas"):
                    for aka in str(r["akas"]).split(";"):
                        aka = aka.strip()
                        if aka and aka.upper() != owner_upper:
                            aliases.add(aka)
                # Collect addresses
                addr_parts = [
                    p for p in [r.get("address1"), r.get("city"), r.get("state")]
                    if p and str(p).strip()
                ]
                if addr_parts:
                    addr = ", ".join(str(p).strip() for p in addr_parts)
                    if r.get("zip_code"):
                        addr += " " + str(r["zip_code"]).strip()
                    addresses.add(addr)

            result["identity"]["aliases"] = sorted(aliases)[:20]
            result["identity"]["addresses"] = sorted(addresses)[:20]
        except Exception:
            logger.opt(exception=True).warning("Personal dossier: civil cases query failed")

        # ── 2. Garnishments ──
        try:
            garn_rows = (
                conn.execute(
                    sa_text("""
                        SELECT case_number, plaintiff_name, defendant_name,
                               garnishee_name, filing_date,
                               case_status_description, writ_issued_date,
                               similarity(defendant_name, :owner_name) AS match_score
                        FROM clerk_garnishment_cases
                        WHERE defendant_name IS NOT NULL
                          AND defendant_name % :owner_name
                          AND similarity(defendant_name, :owner_name) > 0.4
                        ORDER BY filing_date DESC NULLS LAST
                        LIMIT 50
                    """),
                    {"owner_name": owner_upper},
                )
                .mappings()
                .all()
            )
            result["garnishments"] = [dict(r) for r in garn_rows]
        except Exception:
            logger.opt(exception=True).warning("Personal dossier: garnishments query failed")

        # ── 3. Criminal records (table may not exist yet) ──
        try:
            crim_rows = (
                conn.execute(
                    sa_text("""
                        SELECT case_number, case_type, current_status, date_filed,
                               charge_description, count_number, count_level_degree,
                               statute_violation, offense_date,
                               disposition_desc, disposition_date,
                               similarity(last_name, :last_name) AS match_score
                        FROM clerk_criminal_name_index
                        WHERE last_name % :last_name
                          AND similarity(last_name, :last_name) > 0.4
                          AND (
                              :first_name = ''
                              OR first_name IS NULL
                              OR similarity(first_name, :first_name) > 0.3
                          )
                        ORDER BY date_filed DESC NULLS LAST
                        LIMIT 200
                    """),
                    {"last_name": last_name, "first_name": first_name},
                )
                .mappings()
                .all()
            )
            # Group by case_number
            cases_map: dict[str, dict[str, Any]] = {}
            for row in crim_rows:
                r = dict(row)
                cn = r.get("case_number") or "UNKNOWN"
                if cn not in cases_map:
                    cases_map[cn] = {
                        "case_number": cn,
                        "case_type": r.get("case_type"),
                        "current_status": r.get("current_status"),
                        "date_filed": r.get("date_filed"),
                        "charges": [],
                    }
                cases_map[cn]["charges"].append({
                    "count_number": r.get("count_number"),
                    "charge_description": r.get("charge_description"),
                    "count_level_degree": r.get("count_level_degree"),
                    "statute_violation": r.get("statute_violation"),
                    "offense_date": r.get("offense_date"),
                    "disposition_desc": r.get("disposition_desc"),
                    "disposition_date": r.get("disposition_date"),
                })
            result["criminal_cases"] = list(cases_map.values())
        except Exception:
            logger.opt(exception=True).warning(
                "Personal dossier: criminal records query failed (table may not exist)"
            )

        # ── 4. UCC / Federal Liens via sunbiz_flr ──
        try:
            ucc_rows = (
                conn.execute(
                    sa_text("""
                        SELECT DISTINCT ON (sf.doc_number)
                            sf.doc_number,
                            sf.filing_date,
                            sf.filing_status,
                            sf.expiration_date,
                            sp_s.name AS secured_party,
                            similarity(sp.name, :owner_name) AS match_score,
                            CASE
                                WHEN sf.filing_status IN ('A', 'ACTIVE')
                                     AND (sf.expiration_date IS NULL OR sf.expiration_date > CURRENT_DATE)
                                THEN TRUE
                                ELSE FALSE
                            END AS is_active
                        FROM sunbiz_flr_parties sp
                        JOIN sunbiz_flr_filings sf ON sp.doc_number = sf.doc_number
                        LEFT JOIN sunbiz_flr_parties sp_s
                            ON sp_s.doc_number = sf.doc_number
                            AND sp_s.party_role = 'secured'
                        WHERE sp.party_role = 'debtor'
                          AND sp.name IS NOT NULL
                          AND sp.name % :owner_name
                          AND similarity(sp.name, :owner_name) > 0.4
                        ORDER BY sf.doc_number, match_score DESC
                        LIMIT 50
                    """),
                    {"owner_name": owner_upper},
                )
                .mappings()
                .all()
            )
            result["ucc_liens"] = [dict(r) for r in ucc_rows]
        except Exception:
            logger.opt(exception=True).warning("Personal dossier: UCC liens query failed")

        # ── 5. Cross-property ORI documents ──
        try:
            params: dict[str, Any] = {"owner_name": owner_upper}
            strap_clause = ""
            if strap:
                strap_clause = "AND (oe.strap != :strap OR oe.strap IS NULL)"
                params["strap"] = strap

            ori_rows = (
                conn.execute(
                    sa_text(f"""
                        SELECT recording_date,
                               encumbrance_type::text AS encumbrance_type,
                               party1, party2, amount,
                               instrument_number, book, page
                        FROM ori_encumbrances oe
                        WHERE (
                            (oe.party1 % :owner_name AND similarity(oe.party1, :owner_name) > 0.4)
                            OR (oe.party2 % :owner_name AND similarity(oe.party2, :owner_name) > 0.4)
                        )
                        {strap_clause}
                        ORDER BY recording_date DESC NULLS LAST
                        LIMIT 100
                    """),
                    params,
                )
                .mappings()
                .all()
            )
            for row in ori_rows:
                r = dict(row)
                # Determine which party is "other"
                p1_sim = 0.0
                p2_sim = 0.0
                try:
                    p1 = str(r.get("party1") or "").upper()
                    p2 = str(r.get("party2") or "").upper()
                    # Simple check: which party matches the owner more
                    if p1 and owner_upper and owner_upper[:4] in p1:
                        p1_sim = 1.0
                    if p2 and owner_upper and owner_upper[:4] in p2:
                        p2_sim = 1.0
                except Exception:
                    logger.opt(exception=True).debug(
                        "Personal dossier: failed to compare ORI parties for other_party selection"
                    )
                r["other_party"] = r.get("party2") if p1_sim >= p2_sim else r.get("party1")
                result["ori_docs"].append(r)
        except Exception:
            logger.opt(exception=True).warning("Personal dossier: ORI docs query failed")

    return result


@router.get("/{folio}/personal", response_class=HTMLResponse)
async def property_personal(request: Request, folio: str):
    """HTMX partial - personal owner dossier."""
    prop = _pg_property_detail(folio)
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    owner_name = (
        (prop.get("parcel") or {}).get("owner_name")
        or (prop.get("auction") or {}).get("owner_name")
        or ""
    )

    dossier = _pg_personal_dossier(owner_name, prop.get("_strap"), prop.get("_folio_raw"))

    return templates.TemplateResponse(
        "partials/personal.html",
        {
            "request": request,
            "folio": prop.get("folio") or folio,
            "owner_name": owner_name,
            "dossier": dossier,
        },
    )


@router.get("/{folio}/permits", response_class=HTMLResponse)
async def property_permits(request: Request, folio: str):
    """
    HTMX partial - permits and NOCs.
    """
    prop = _pg_property_detail(folio)
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    permits = _pg_permits_for_property(prop.get("_foreclosure_id") or 0)
    nocs_value = prop.get("nocs")
    nocs = nocs_value if isinstance(nocs_value, list) else []

    return templates.TemplateResponse(
        "partials/permits.html", {"request": request, "permits": permits, "nocs": nocs, "folio": prop.get("folio") or folio}
    )


@router.get("/{folio}/chain", response_class=HTMLResponse)
async def property_chain_of_title(request: Request, folio: str):
    """
    HTMX partial - chain of title for a property.
    """
    prop = _pg_property_detail(folio)

    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    chain_of_title = _pg_chain_for_property(
        identifier=folio,
        case_number=(prop.get("auction") or {}).get("case_number"),
    )
    chain_gaps = _pg_chain_gaps_for_property(
        identifier=folio,
        case_number=(prop.get("auction") or {}).get("case_number"),
    )
    if not chain_of_title:
        chain_of_title = prop.get("chain", [])
    real_folio = prop.get("folio") or folio

    docs = _pg_documents_for_property(real_folio)
    doc_index = _build_document_token_index(docs)

    # Enhance chain with local document links and clerk search fallback URLs.
    for item in chain_of_title:
        item["document_id"] = None
        instrument_value = item.get("acquisition_instrument") or item.get("instrument_number") or item.get("instrument")
        for token in _extract_instrument_tokens(instrument_value):
            doc_id = doc_index.get(token)
            if doc_id is not None:
                item["document_id"] = doc_id
                break
        item["instrument_url"] = _instrument_search_url(instrument_value)

    # Compute chain duration for the 30-year coverage indicator.
    chain_years = None
    chain_start_year = None
    chain_end_year = None
    if chain_of_title:
        dates = []
        for item in chain_of_title:
            raw = item.get("acquisition_date")
            if raw is None:
                continue
            if isinstance(raw, (date, datetime)):
                dates.append(raw)
            else:
                text = str(raw).strip()[:10]
                with contextlib.suppress(ValueError, TypeError):
                    dates.append(datetime.strptime(text, "%Y-%m-%d").date())
        if dates:
            chain_start_year = min(dates).year
            chain_end_year = max(dates).year
            chain_years = chain_end_year - chain_start_year

    return templates.TemplateResponse(
        "partials/chain_of_title.html",
        {
            "request": request,
            "chain_of_title": chain_of_title,
            "chain_gaps": chain_gaps,
            "folio": real_folio,
            "chain_years": chain_years,
            "chain_start_year": chain_start_year,
            "chain_end_year": chain_end_year,
        },
    )


@router.get("/{folio}/judgment", response_class=HTMLResponse)
async def property_judgment(request: Request, folio: str):
    """
    HTMX partial - extracted final judgment data.
    """
    prop = _pg_property_detail(folio)
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    auction = prop.get("auction", {})
    judgment = {
        "case_number": auction.get("case_number"),
        "foreclosure_type": auction.get("foreclosure_type"),
        "lis_pendens_date": auction.get("lis_pendens_date"),
        "plaintiff": auction.get("plaintiff"),
        "defendant": auction.get("defendant"),
        "final_judgment_amount": auction.get("final_judgment_amount"),
        "judgment_extracted_at": auction.get("judgment_extracted_at"),
        "extracted_judgment_data": auction.get("extracted_judgment_data"),
    }

    return templates.TemplateResponse(
        "partials/judgment.html", {"request": request, "judgment": judgment, "folio": prop.get("folio") or folio}
    )


@router.get("/{folio}/comparables", response_class=HTMLResponse)
async def property_comparables(request: Request, folio: str, years: int = 3):
    """
    HTMX partial - comparable sales from PG.
    """
    pg = get_pg_queries()
    comps = []
    subdivision = None
    if pg.available:
        comps = pg.get_comparable_sales(folio, years=years)
        subdivision = pg.get_subdivision_info(folio)

    return templates.TemplateResponse(
        "partials/comparables.html",
        {
            "request": request,
            "comparables": comps,
            "subdivision": subdivision,
            "folio": folio,
            "years": years,
            "pg_available": pg.available,
        },
    )


def _sanitize_folio(folio: str) -> str:
    return (
        folio.replace("-", "")
        .replace(" ", "")
        .replace(":", "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace(",", "")
        .replace("#", "")
    )


@router.get("/{folio}/doc/{doc_id}")
async def property_document_file(folio: str, doc_id: int):
    """
    Serve a document file by its DB id.
    Checks data/Foreclosure/{case_number}/documents/ first, then data/properties/{folio}/.
    """
    docs = _pg_documents_for_property(folio)
    doc = next((d for d in docs if d.get("id") == doc_id), None)
    if not doc or not doc.get("file_path"):
        raise HTTPException(status_code=404, detail="Document not found")

    project_root = Path(__file__).resolve().parents[3]
    file_path = (project_root / doc["file_path"]).resolve()

    # Prevent path traversal — must be under project data dir
    data_dir = (project_root / "data").resolve()
    if not str(file_path).startswith(str(data_dir)):
        raise HTTPException(status_code=404, detail="Invalid document path")
    if not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found on disk")

    return FileResponse(path=file_path, filename=file_path.name)


@router.get("/{folio}/documents/{filename:path}")
async def serve_document_by_name(folio: str, filename: str):
    """
    Serve a document file by filename for a property.
    Looks in data/Foreclosure/{case_number}/documents/ and data/properties/{folio}/.
    """
    project_root = Path(__file__).resolve().parents[3]
    data_dir = (project_root / "data").resolve()

    # Sanitize filename
    if ".." in filename or filename.startswith("/"):
        raise HTTPException(status_code=400, detail="Invalid filename")

    # Try Foreclosure path first — look up case_number(s) from PG
    for case_num in _pg_case_numbers_for_property(folio):
        candidate = data_dir / "Foreclosure" / case_num / "documents" / filename
        if candidate.resolve().is_file() and str(candidate.resolve()).startswith(str(data_dir)):
            return FileResponse(path=candidate.resolve(), filename=filename)

    # Fallback: data/properties/{folio}/
    safe_folio = _sanitize_folio(folio)
    fallback = data_dir / "properties" / safe_folio / filename
    if fallback.resolve().is_file() and str(fallback.resolve()).startswith(str(data_dir)):
        return FileResponse(path=fallback.resolve(), filename=filename)

    raise HTTPException(status_code=404, detail="File not found on disk")


@router.get("/{folio}/photos/{filename}")
async def serve_photo(folio: str, filename: str):
    """Serve a locally downloaded property photo."""
    # Path traversal protection
    if ".." in filename or "/" in filename or filename.startswith("."):
        raise HTTPException(status_code=400, detail="Invalid filename")

    project_root = Path(__file__).resolve().parents[3]
    data_dir = (project_root / "data").resolve()

    # Look up case_number(s) from PG
    for case_num in _pg_case_numbers_for_property(folio):
        candidate = data_dir / "Foreclosure" / case_num / "photos" / filename
        resolved = candidate.resolve()
        if resolved.is_file() and str(resolved).startswith(str(data_dir)):
            return FileResponse(path=resolved, filename=filename)

    raise HTTPException(status_code=404, detail="Photo not found on disk")


@router.get("/{folio}/title-report", response_class=HTMLResponse)
async def property_title_report(request: Request, folio: str):
    """
    Generate a printable Title Report.
    """
    prop = _pg_property_detail(folio)
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    return templates.TemplateResponse(
        "title_report.html", {"request": request, "property": prop, "generated_date": today_local().strftime("%B %d, %Y")}
    )
