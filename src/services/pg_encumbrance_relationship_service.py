"""Relationship-driven encumbrance recovery from extracted JSON.

Architectural purpose
---------------------
This service is the bridge between "we extracted good JSON" and "the pipeline
used that structure to find more documents and improve downstream analysis."

It intentionally runs *after* ORI discovery and PDF extraction. The broad ORI
search step is still responsible for parcel-scoped discovery. This service is a
bounded second pass that turns high-confidence references from extracted JSON
into exact ORI lookups:

- satisfactions/releases -> parent instrument
- assignments -> base mortgage instrument
- lis pendens -> foreclosed instrument
- mechanic's liens -> referenced notice of commencement
- judgments -> foreclosed mortgage + lis pendens refs

The service does not add tables or columns. It only reuses the current
pipeline's existing persistence surfaces:

- ``ori_encumbrances.extracted_data``
- ``foreclosures.judgment_data``
- ``ori_encumbrances.current_holder``
- ``PgOriService.discover_exact_references()``
- ``PgEncumbranceExtractionService.run()``

The loop is intentionally capped. One pass can save newly discovered docs,
trigger extraction on those docs, and then run one more relationship pass so
those new payloads can contribute additional exact references.
"""

from __future__ import annotations

import json
from typing import Any

from loguru import logger
from sqlalchemy import text

from src.services.pg_encumbrance_extraction_service import PgEncumbranceExtractionService
from src.services.pg_ori_service import PgOriService
from sunbiz.db import get_engine, resolve_pg_dsn


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _recording_ref_parts(ref: Any) -> tuple[str | None, tuple[str, str] | None]:
    payload = _as_dict(ref)
    if not payload:
        return None, None
    instrument = str(payload.get("instrument_number") or "").strip() or None
    book = str(payload.get("recording_book") or payload.get("book") or "").strip()
    page = str(payload.get("recording_page") or payload.get("page") or "").strip()
    book_page = (book, page) if book and page else None
    return instrument, book_page


class PgEncumbranceRelationshipService:
    """Run exact-reference recovery and holder propagation from extracted JSON."""

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = resolve_pg_dsn(dsn)
        self.engine = get_engine(self.dsn)
        self.ori_service = PgOriService(dsn=self.dsn)
        self.extraction_service = PgEncumbranceExtractionService(dsn=self.dsn)

    def run(
        self,
        *,
        limit: int | None = None,
        straps: list[str] | None = None,
        foreclosure_ids: list[int] | None = None,
        max_passes: int = 2,
    ) -> dict[str, Any]:
        """Recover exact-reference documents and propagate holder changes."""

        targets = self._find_targets(
            limit=limit,
            straps=straps,
            foreclosure_ids=foreclosure_ids,
        )
        if not targets:
            return {"skipped": True, "reason": "no_relationship_targets"}

        aggregate = {
            "targets": len(targets),
            "passes": 0,
            "leads_total": 0,
            "local_matches": 0,
            "searched_instruments": 0,
            "searched_book_pages": 0,
            "docs_found": 0,
            "saved": 0,
            "linked_satisfactions": 0,
            "linked_modifications": 0,
            "holder_updates": 0,
            "errors": 0,
            "reextract_extracted": 0,
            "reextract_cached": 0,
            "reextract_errors": 0,
            "reextract_skipped": 0,
            "reextract_ori_id_backfilled": 0,
        }
        all_target_results: list[dict[str, Any]] = []
        pending_targets = targets

        for pass_index in range(max(1, max_passes)):
            aggregate["passes"] += 1
            changed_straps: set[str] = set()
            changed_foreclosure_ids: set[int] = set()
            pass_results: list[dict[str, Any]] = []

            for target in pending_targets:
                try:
                    result = self._process_target(target)
                except Exception:
                    logger.exception(
                        "Relationship recovery failed for foreclosure_id={} strap={}",
                        target.get("foreclosure_id"),
                        target.get("strap"),
                    )
                    aggregate["errors"] += 1
                    continue

                pass_results.append(result)
                aggregate["leads_total"] += int(result.get("leads_total", 0))
                aggregate["local_matches"] += int(result.get("local_matches", 0))
                aggregate["searched_instruments"] += int(result.get("searched_instruments", 0))
                aggregate["searched_book_pages"] += int(result.get("searched_book_pages", 0))
                aggregate["docs_found"] += int(result.get("docs_found", 0))
                aggregate["saved"] += int(result.get("saved", 0))
                aggregate["linked_satisfactions"] += int(result.get("linked_satisfactions", 0))
                aggregate["linked_modifications"] += int(result.get("linked_modifications", 0))
                aggregate["holder_updates"] += int(result.get("holder_updates", 0))
                if bool(result.get("changed")):
                    strap = str(result.get("strap") or "").strip()
                    foreclosure_id = int(result.get("foreclosure_id") or 0)
                    if strap:
                        changed_straps.add(strap)
                    if foreclosure_id:
                        changed_foreclosure_ids.add(foreclosure_id)

            all_target_results.extend(pass_results)

            if not changed_straps or pass_index + 1 >= max(1, max_passes):
                break

            reextract = self.extraction_service.run(straps=sorted(changed_straps))
            reextract_errs = int(reextract.get("errors", 0))
            aggregate["reextract_extracted"] += int(reextract.get("extracted", 0))
            aggregate["reextract_cached"] += int(reextract.get("cached", 0))
            aggregate["reextract_errors"] += reextract_errs
            aggregate["reextract_skipped"] += int(reextract.get("skipped", 0))
            aggregate["reextract_ori_id_backfilled"] += int(reextract.get("ori_id_backfilled", 0))
            aggregate["errors"] += reextract_errs

            pending_targets = self._find_targets(
                limit=None,
                straps=sorted(changed_straps),
                foreclosure_ids=sorted(changed_foreclosure_ids) or None,
            )

        aggregate["per_target"] = all_target_results
        return aggregate

    def _find_targets(
        self,
        *,
        limit: int | None,
        straps: list[str] | None,
        foreclosure_ids: list[int] | None,
    ) -> list[dict[str, Any]]:
        """Find active foreclosures whose extracted JSON can drive exact search."""

        sql = """
            SELECT DISTINCT
                   f.foreclosure_id,
                   f.case_number_raw AS case_number,
                   f.strap,
                   f.folio,
                   f.judgment_data
            FROM foreclosures f
            WHERE f.archived_at IS NULL
              AND f.step_ori_searched IS NOT NULL
              AND f.strap IS NOT NULL
              AND btrim(f.strap) <> ''
              AND (
                  f.judgment_data IS NOT NULL
                  OR EXISTS (
                      SELECT 1
                      FROM ori_encumbrances oe
                      WHERE oe.strap = f.strap
                        AND oe.extracted_data IS NOT NULL
                        AND oe.encumbrance_type IN (
                            'assignment',
                            'satisfaction',
                            'release',
                            'lis_pendens',
                            'lien'
                        )
                  )
              )
        """
        params: dict[str, Any] = {}
        if straps:
            sql += " AND f.strap = ANY(:straps)"
            params["straps"] = list(straps)
        if foreclosure_ids:
            sql += " AND f.foreclosure_id = ANY(:foreclosure_ids)"
            params["foreclosure_ids"] = list(foreclosure_ids)
        sql += " ORDER BY f.auction_date NULLS LAST, f.foreclosure_id"
        if limit:
            sql += " LIMIT :limit"
            params["limit"] = limit

        with self.engine.connect() as conn:
            rows = conn.execute(text(sql), params).mappings().all()
        return [dict(row) for row in rows]

    def _load_rows_for_strap(self, strap: str) -> list[dict[str, Any]]:
        sql = text("""
            SELECT id, encumbrance_type, instrument_number, book, page,
                   current_holder, extracted_data, recording_date
            FROM ori_encumbrances
            WHERE strap = :strap
            ORDER BY recording_date NULLS LAST, id
        """)
        with self.engine.connect() as conn:
            rows = conn.execute(sql, {"strap": strap}).mappings().all()
        return [dict(row) for row in rows]

    @staticmethod
    def _known_reference_sets(rows: list[dict[str, Any]]) -> tuple[set[str], set[tuple[str, str]]]:
        instruments: set[str] = set()
        book_pages: set[tuple[str, str]] = set()
        for row in rows:
            instrument = str(row.get("instrument_number") or "").strip()
            if instrument:
                instruments.add(instrument)
            book = str(row.get("book") or "").strip()
            page = str(row.get("page") or "").strip()
            if book and page:
                book_pages.add((book, page))
        return instruments, book_pages

    def _process_target(self, target: dict[str, Any]) -> dict[str, Any]:
        foreclosure_id = int(target["foreclosure_id"])
        strap = str(target["strap"] or "").strip()
        folio = str(target.get("folio") or "").strip() or None
        judgment_data = _as_dict(target.get("judgment_data"))

        rows = self._load_rows_for_strap(strap)
        known_instruments, known_book_pages = self._known_reference_sets(rows)
        leads = self._collect_leads(rows=rows, judgment_data=judgment_data)

        missing_instruments: set[str] = set()
        missing_book_pages: set[tuple[str, str]] = set()
        local_matches = 0
        for lead in leads:
            instrument = str(lead.get("instrument") or "").strip()
            book_page = lead.get("book_page")
            if instrument and instrument in known_instruments:
                local_matches += 1
                continue
            if book_page and book_page in known_book_pages:
                local_matches += 1
                continue
            if instrument:
                missing_instruments.add(instrument)
            if isinstance(book_page, tuple):
                missing_book_pages.add(book_page)

        discovery = self.ori_service.discover_exact_references(
            strap=strap,
            folio=folio,
            instruments=sorted(missing_instruments),
            book_pages=sorted(missing_book_pages),
        )
        holder_updates = self._apply_holder_updates(strap=strap, judgment_data=judgment_data)

        changed = bool(
            int(discovery.get("saved", 0))
            or int(discovery.get("linked_satisfactions", 0))
            or int(discovery.get("linked_modifications", 0))
            or holder_updates
        )
        return {
            "foreclosure_id": foreclosure_id,
            "strap": strap,
            "leads_total": len(leads),
            "local_matches": local_matches,
            "searched_instruments": int(discovery.get("searched_instruments", 0)),
            "searched_book_pages": int(discovery.get("searched_book_pages", 0)),
            "docs_found": int(discovery.get("docs_found", 0)),
            "saved": int(discovery.get("saved", 0)),
            "linked_satisfactions": int(discovery.get("linked_satisfactions", 0)),
            "linked_modifications": int(discovery.get("linked_modifications", 0)),
            "holder_updates": holder_updates,
            "changed": changed,
        }

    def _collect_leads(
        self,
        *,
        rows: list[dict[str, Any]],
        judgment_data: dict[str, Any],
    ) -> list[dict[str, Any]]:
        leads: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()

        def add_lead(label: str, ref: Any) -> None:
            instrument, book_page = _recording_ref_parts(ref)
            instrument_value = instrument or ""
            book_value, page_value = book_page or ("", "")
            key = (label, instrument_value, f"{book_value}/{page_value}")
            if key in seen:
                return
            if not instrument and not book_page:
                return
            seen.add(key)
            leads.append({
                "label": label,
                "instrument": instrument,
                "book_page": book_page,
            })

        for row in rows:
            extracted = _as_dict(row.get("extracted_data"))
            enc_type = str(row.get("encumbrance_type") or "")
            if enc_type in {"satisfaction", "release"}:
                add_lead("satisfaction_parent", extracted.get("parent_instrument"))
            elif enc_type == "assignment":
                add_lead("assignment_parent", extracted.get("parent_instrument"))
            elif enc_type == "lis_pendens":
                add_lead("lp_foreclosed_instrument", extracted.get("foreclosed_instrument"))
            elif enc_type == "lien":
                add_lead("lien_referenced_noc", extracted.get("referenced_noc"))

        add_lead("judgment_foreclosed_mortgage", judgment_data.get("foreclosed_mortgage"))
        add_lead("judgment_lis_pendens", judgment_data.get("lis_pendens"))

        return leads

    def _apply_holder_updates(self, *, strap: str, judgment_data: dict[str, Any]) -> int:
        """Update base encumbrance holders from assignment and judgment payloads."""

        rows = self._load_rows_for_strap(strap)
        bases_by_instrument: dict[str, int] = {}
        bases_by_book_page: dict[tuple[str, str], int] = {}
        updates: list[tuple[int, str]] = []

        for row in rows:
            if row.get("encumbrance_type") in {"assignment", "satisfaction", "release", "noc", "lis_pendens", "other"}:
                continue
            base_id = int(row["id"])
            instrument = str(row.get("instrument_number") or "").strip()
            if instrument:
                bases_by_instrument[instrument] = base_id
            book = str(row.get("book") or "").strip()
            page = str(row.get("page") or "").strip()
            if book and page:
                bases_by_book_page[(book, page)] = base_id

        # Judgment holder first — serves as baseline that assignments can override.
        # Assignments are recorded after the mortgage origination and reflect the
        # most recent holder, so they must come last in the updates list.
        foreclosed = _as_dict(judgment_data.get("foreclosed_mortgage"))
        current_holder = str(foreclosed.get("current_holder") or "").strip()
        if current_holder:
            instrument, book_page = _recording_ref_parts(foreclosed)
            base_id = None
            if instrument:
                base_id = bases_by_instrument.get(instrument)
            if base_id is None and book_page:
                base_id = bases_by_book_page.get(book_page)
            if base_id:
                updates.append((base_id, current_holder))

        for row in rows:
            if row.get("encumbrance_type") != "assignment":
                continue
            extracted = _as_dict(row.get("extracted_data"))
            assignee = str(extracted.get("assignee") or "").strip()
            instrument, book_page = _recording_ref_parts(extracted.get("parent_instrument"))
            base_id = None
            if instrument:
                base_id = bases_by_instrument.get(instrument)
            if base_id is None and book_page:
                base_id = bases_by_book_page.get(book_page)
            if base_id and assignee:
                updates.append((base_id, assignee))

        if not updates:
            return 0

        updated = 0
        with self.engine.begin() as conn:
            for encumbrance_id, holder in updates:
                result = conn.execute(
                    text("""
                        UPDATE ori_encumbrances
                        SET current_holder = :holder,
                            updated_at = NOW()
                        WHERE id = :id
                          AND current_holder IS DISTINCT FROM :holder
                    """),
                    {"id": encumbrance_id, "holder": holder},
                )
                updated += int(result.rowcount or 0)
        if updated:
            logger.info("Updated {} base encumbrance holder(s) for strap={}", updated, strap)
        return updated
