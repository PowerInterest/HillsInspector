"""Title recovery and fixup service.

This is the primary service responsible for all data fixups and recoveries
related to chain of title. It addresses the inherent weakness of raw bulk
county data (e.g., HCPA allsales) missing records or parties.

Importantly, this service NEVER modifies the raw bulk data tables directly.
All recovered information, whether entirely missing deeds or backfilled
grantor/grantee names, is safely stored in the `foreclosure_title_events` table
as an overlay. The `fn_title_chain` SQL view then seamlessly stitches these
events together with the raw data.

This service currently performs 2 main gap-fillers:
1. ORI_DEED_SEARCH: Identifies completely missing transfers (temporal or name gaps)
   and searches the Clerk's Official Records to insert the missing links.
2. ORI_DEED_BACKFILL: Queries HCPA deeds that exist but are missing their
   party names (grantor/grantee) and retrieves them from the Clerk API.
"""

from __future__ import annotations

import time
from datetime import date
from typing import Any

from loguru import logger
from sqlalchemy import text

from src.db.type_normalizer import normalize_document_type
from src.services.pg_ori_service import PgOriService
from sunbiz.db import get_engine, resolve_pg_dsn

# Document types that represent ownership transfers
_DEED_TYPES = frozenset({
    "warranty_deed",
    "quit_claim_deed",
    "certificate_of_title",
    "deed",
    "tax_deed",
})


class PgTitleBreakService:
    """Service to search ORI/PAV for title gap-fills and party backfills."""

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = resolve_pg_dsn(dsn)
        self.engine = get_engine(self.dsn)
        self._ori = PgOriService(dsn=self.dsn)

    def run(self, *, limit: int | None = None) -> dict[str, Any]:
        targets = self._find_targets(limit)
        if not targets:
            return {"skipped": True, "reason": "no_targets"}

        logger.info(f"title_breaks: {len(targets)} foreclosures to process")

        total_gaps = 0
        total_inserted = 0
        errors = 0

        for t in targets:
            try:
                gaps_found, inserted = self._process_one(t)
                total_gaps += gaps_found
                total_inserted += inserted
            except Exception as exc:
                errors += 1
                logger.error(
                    "title_breaks: error on foreclosure_id={} folio={}: {}",
                    t["foreclosure_id"],
                    t["folio"],
                    exc,
                )

        result = {
            "targets": len(targets),
            "gaps_found": total_gaps,
            "deeds_inserted": total_inserted,
            "backfilled": self._backfill_deed_parties(limit),
            "errors": errors,
        }
        logger.info(f"title_breaks: {result}")
        return result

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _find_targets(self, limit: int | None) -> list[dict[str, Any]]:
        """Active foreclosures with title events but no ORI_DEED_SEARCH yet."""
        sql = """
            SELECT DISTINCT f.foreclosure_id, f.case_number_raw,
                   f.case_number_norm, f.strap, f.folio
            FROM foreclosures f
            JOIN foreclosure_title_events fte
              ON fte.foreclosure_id = f.foreclosure_id
            WHERE f.archived_at IS NULL
              AND f.folio IS NOT NULL
              AND btrim(f.folio) <> ''
              AND NOT EXISTS (
                  SELECT 1 FROM foreclosure_title_events e2
                  WHERE e2.foreclosure_id = f.foreclosure_id
                    AND e2.event_source = 'ORI_DEED_SEARCH'
              )
            ORDER BY f.foreclosure_id
        """
        if limit:
            sql += f" LIMIT {int(limit)}"

        with self.engine.connect() as conn:
            rows = conn.execute(text(sql)).mappings().fetchall()
        return [dict(r) for r in rows]

    def _process_one(self, target: dict[str, Any]) -> tuple[int, int]:
        """Process a single foreclosure: find gaps, search ORI, insert deeds.

        Returns (gaps_found, deeds_inserted).
        """
        folio = target["folio"]

        with self.engine.connect() as conn:
            gaps = (
                conn.execute(
                    text("""
                    SELECT gap_type, expected_from_party, observed_to_party,
                           missing_from_date, missing_to_date
                    FROM fn_title_chain_gaps(:folio)
                """),
                    {"folio": folio},
                )
                .mappings()
                .fetchall()
            )

        if not gaps:
            return 0, 0

        all_deeds: list[dict[str, Any]] = []

        for gap in gaps:
            party = gap["expected_from_party"] or gap["observed_to_party"]
            if not party:
                continue

            from_date = gap["missing_from_date"] or date(1970, 1, 1)
            import datetime as dt

            to_date = gap["missing_to_date"] or dt.datetime.now(dt.UTC).date()
            # Ensure date types (PG may return date objects already)
            if isinstance(from_date, str):
                from_date = date.fromisoformat(from_date)
            if isinstance(to_date, str):
                to_date = date.fromisoformat(to_date)

            stats: dict[str, int] = {
                "api_calls": 0,
                "retries": 0,
                "truncated": 0,
                "unresolved_truncations": 0,
            }
            try:
                results = self._ori.search_party_pav(
                    party,
                    stats,
                    from_date=from_date,
                    to_date=to_date,
                    split_on_truncated=True,
                )
            except Exception as exc:
                logger.warning(
                    "title_breaks: PAV search failed for party={!r} folio={}: {}",
                    party,
                    folio,
                    exc,
                )
                continue

            for doc in results:
                raw_type = doc.get("DocType") or ""
                if normalize_document_type(raw_type) in _DEED_TYPES:
                    all_deeds.append(doc)

            # Be polite to PAV
            time.sleep(0.5)

        if not all_deeds:
            return len(gaps), 0

        inserted = self._insert_deeds(target, all_deeds)
        return len(gaps), inserted

    def _insert_deeds(self, target: dict[str, Any], deeds: list[dict[str, Any]]) -> int:
        """Insert found deeds into foreclosure_title_events."""
        rows_to_insert = []
        seen_instruments: set[str] = set()

        for doc in deeds:
            instrument = (doc.get("Instrument") or "").strip()
            if not instrument or instrument in seen_instruments:
                continue
            seen_instruments.add(instrument)

            record_date = PgOriService.parse_date(doc.get("RecordDate") or doc.get("record_date"))
            if not record_date:
                continue

            raw_type = doc.get("DocType") or ""
            parties_one = doc.get("PartiesOne") or []
            parties_two = doc.get("PartiesTwo") or []

            rows_to_insert.append({
                "foreclosure_id": target["foreclosure_id"],
                "case_number_raw": target["case_number_raw"],
                "case_number_norm": target.get("case_number_norm"),
                "folio": target["folio"],
                "strap": target.get("strap"),
                "event_date": record_date,
                "event_source": "ORI_DEED_SEARCH",
                "event_subtype": normalize_document_type(raw_type),
                "instrument_number": instrument,
                "grantor": "; ".join(parties_one)[:1000] if parties_one else None,
                "grantee": "; ".join(parties_two)[:1000] if parties_two else None,
                "description": raw_type,
            })

        if not rows_to_insert:
            return 0

        with self.engine.begin() as conn:
            # Avoid duplicates on re-run edge cases
            result = conn.execute(
                text("""
                    INSERT INTO foreclosure_title_events (
                        foreclosure_id, case_number_raw, case_number_norm,
                        folio, strap,
                        event_date, event_source, event_subtype,
                        instrument_number, grantor, grantee, description
                    )
                    SELECT
                        :foreclosure_id, :case_number_raw, :case_number_norm,
                        :folio, :strap,
                        :event_date::date, :event_source, :event_subtype,
                        :instrument_number, :grantor, :grantee, :description
                    WHERE NOT EXISTS (
                        SELECT 1 FROM foreclosure_title_events
                        WHERE foreclosure_id = :foreclosure_id
                          AND instrument_number = :instrument_number
                          AND event_source = 'ORI_DEED_SEARCH'
                    )
                """),
                rows_to_insert,
            )
            return result.rowcount or 0

    def _backfill_deed_parties(self, limit: int | None = None) -> int:
        """Fetch missing grantor/grantee from PAV for active-foreclosure deeds.

        Queries hcpa_allsales rows tied to active foreclosures that have a
        doc_num but NULL grantor/grantee. It hits the PAV instrument search API
        and inserts results into foreclosure_title_events so fn_title_chain
        can resolve party names.
        """
        import requests as _requests

        pav_url = "https://publicaccess.hillsclerk.com/api/OfficialRecordsDirectSearch/AdvancedSearch"
        pav_headers = {"Content-Type": "application/json"}

        # First, find foreclosure folks missing a deed party...
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT 
                        s.doc_num, 
                        MAX(s.sale_date) AS sale_date,
                        MAX(f.foreclosure_id) AS foreclosure_id,
                        MAX(s.folio) AS folio,
                        MAX(f.case_number_raw) AS case_number_raw,
                        MAX(f.case_number_norm) AS case_number_norm,
                        MAX(f.strap) AS strap
                    FROM hcpa_allsales s
                    JOIN foreclosures f ON s.folio = f.folio
                    WHERE f.archived_at IS NULL
                      AND (s.grantor IS NULL OR s.grantee IS NULL)
                      AND s.doc_num IS NOT NULL
                      AND trim(s.doc_num) <> ''
                      AND NOT EXISTS (
                          SELECT 1 FROM foreclosure_title_events e
                          WHERE e.instrument_number = s.doc_num
                            AND e.event_source = 'ORI_DEED_BACKFILL'
                      )
                    GROUP BY s.doc_num
                    ORDER BY MAX(s.sale_date) DESC NULLS LAST
                """)
            ).fetchall()

        if not rows:
            return 0

        success = 0
        targets = rows[:limit] if limit else rows
        sess = _requests.Session()

        for row in targets:
            doc_num = (row.doc_num or "").strip()
            if not doc_num:
                continue

            payload = {
                "MatchAnySearchWord": True,
                "InstrumentNumberSearchValue": doc_num,
                "IsExactNameSearchMode": False,
            }
            parsed = None
            for _ in range(1, 4):
                try:
                    resp = sess.post(pav_url, json=payload, headers=pav_headers, timeout=10)
                    resp.raise_for_status()
                    data = resp.json()
                    docs = data.get("Documents", [])
                    if docs:
                        parsed = self._parse_pav_parties(docs, doc_num)
                    break
                except _requests.RequestException:
                    time.sleep(1)

            if parsed is None or (not parsed["from_text"] and not parsed["to_text"]):
                time.sleep(0.5)
                continue

            with self.engine.begin() as conn:
                result = conn.execute(
                    text("""
                        INSERT INTO foreclosure_title_events (
                            foreclosure_id, case_number_raw, case_number_norm,
                            folio, strap, event_date, event_source, event_subtype,
                            instrument_number, grantor, grantee, description
                        )
                        SELECT
                            :foreclosure_id, :case_number_raw, :case_number_norm,
                            :folio, :strap, :event_date::date, :event_source, :event_subtype,
                            :instrument_number, :grantor, :grantee, :description
                        WHERE NOT EXISTS (
                            SELECT 1 FROM foreclosure_title_events
                            WHERE instrument_number = :instrument_number
                              AND event_source = 'ORI_DEED_BACKFILL'
                        )
                    """),
                    {
                        "foreclosure_id": row.foreclosure_id,
                        "case_number_raw": row.case_number_raw,
                        "case_number_norm": row.case_number_norm,
                        "folio": row.folio,
                        "strap": row.strap,
                        "event_date": self._parse_pav_record_date(parsed["record_date"]) or (row.sale_date or date(1970, 1, 1)),
                        "event_source": "ORI_DEED_BACKFILL",
                        "event_subtype": normalize_document_type(parsed["doc_type"] or "") or None,
                        "instrument_number": doc_num,
                        "grantor": parsed["from_text"],
                        "grantee": parsed["to_text"],
                        "description": parsed["doc_type"] or "Backfilled Deed",
                    },
                )
            if (result.rowcount or 0) > 0:
                success += 1
            time.sleep(1.0)

        return success

    @staticmethod
    def _parse_pav_parties(rows: list[dict], target_instrument: str) -> dict[str, str | None] | None:
        parties_from: list[str] = []
        parties_to: list[str] = []
        doc_type = ""
        record_date = ""
        for row in rows:
            cols = row.get("DisplayColumnValues") or []
            if len(cols) < 9:
                continue
            vals = [str(c.get("Value") or "").strip() for c in cols[:9]]
            vals.extend([""] * (9 - len(vals)))
            person_type, name, instrument = vals[0].upper(), vals[1], vals[8]
            if instrument != target_instrument:
                continue
            if not doc_type:
                doc_type = vals[3]
            if not record_date:
                record_date = vals[2]
            if name:
                if "2" in person_type or "GRANTEE" in person_type:
                    if name not in parties_to:
                        parties_to.append(name)
                elif name not in parties_from:
                    parties_from.append(name)
        if not parties_from and not parties_to:
            return None
        return {
            "doc_type": doc_type,
            "record_date": record_date,
            "from_text": "; ".join(parties_from)[:1000] or None,
            "to_text": "; ".join(parties_to)[:1000] or None,
        }

    @staticmethod
    def _parse_pav_record_date(value: str | None) -> date | None:
        if not value:
            return None
        text_value = value.strip()
        if not text_value:
            return None
        import datetime as dt

        for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y", "%Y-%m-%d"):
            try:
                return dt.datetime.strptime(text_value, fmt).date()
            except ValueError:
                continue
        return None
