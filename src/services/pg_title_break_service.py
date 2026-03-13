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

import datetime as dt
import html
import re
import time
from datetime import date
from typing import Any

from loguru import logger
from scourgify import normalize_address_record
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

_HIGH_VOLUME_BUILDER_TOKENS = (
    "LENNAR",
    "PULTE",
    "CALATLANTIC",
    "LGI HOMES",
    "CENTEX",
    "WCI COMMUNITIES",
    "KB HOME",
    "DR HORTON",
    "D R HORTON",
    "MERITAGE",
    "TAYLOR MORRISON",
    "RICHMOND AMERICAN",
    "BEAZER",
    "HOLIDAY BUILDERS",
    "RYLAND",
    "STANDARD PACIFIC",
    "ASHTON WOODS",
    "DAVID WEEKLEY",
    "WESTBAY",
)

_SEARCH_NO_RESULT_RETRY_DAYS = 14
_MAX_CONTEXT_SEARCH_NAMES = 6
_HISTORICAL_CONTEXT_LIMIT = 8
_ADDRESS_SUFFIX_TOKENS = frozenset({
    "ST",
    "AVE",
    "BLVD",
    "DR",
    "RD",
    "LN",
    "CT",
    "CIR",
    "PL",
    "PKWY",
    "TER",
    "TRL",
    "HWY",
    "WAY",
})
_ADDRESS_UNIT_TOKENS = frozenset({
    "APT",
    "UNIT",
    "STE",
    "SUITE",
    "#",
})
_ADDRESS_DIRECTION_TOKENS = frozenset({
    "N",
    "S",
    "E",
    "W",
    "NE",
    "NW",
    "SE",
    "SW",
})


class PgTitleBreakService:
    """Service to search ORI/PAV for title gap-fills and party backfills."""

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = resolve_pg_dsn(dsn)
        self.engine = get_engine(self.dsn)
        self._ori = PgOriService(dsn=self.dsn)
        self._case_party_context_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self._historical_party_context_cache: dict[tuple[str, str, str], list[dict[str, Any]]] = {}

    def run(
        self,
        *,
        limit: int | None = None,
        foreclosure_id: int | None = None,
        case_number: str | None = None,
    ) -> dict[str, Any]:
        sentinel_skips = self._find_recent_sentinel_skips(
            foreclosure_id=foreclosure_id,
            case_number=case_number,
        )
        self._log_recent_sentinel_skips(sentinel_skips)

        targets = self._find_targets(
            limit,
            foreclosure_id=foreclosure_id,
            case_number=case_number,
        )
        if not targets:
            if sentinel_skips:
                return {
                    "skipped": True,
                    "reason": "recent_search_no_result_sentinels",
                    "recent_sentinel_skip_count": len(sentinel_skips),
                }
            return {"skipped": True, "reason": "no_targets"}

        logger.info(f"title_breaks: {len(targets)} foreclosures to process")

        total_gaps = 0
        total_inserted = 0
        total_sentinels = 0
        errors = 0

        for t in targets:
            try:
                gaps_found, inserted, sentinels = self._process_one(t)
                total_gaps += gaps_found
                total_inserted += inserted
                total_sentinels += sentinels
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
            "sentinels_inserted": total_sentinels,
            "recent_sentinel_skip_count": len(sentinel_skips),
            "backfilled": self._backfill_deed_parties(
                limit,
                foreclosure_id=foreclosure_id,
                case_number=case_number,
            ),
            "errors": errors,
        }
        logger.info(f"title_breaks: {result}")
        return result

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _find_targets(
        self,
        limit: int | None,
        *,
        foreclosure_id: int | None = None,
        case_number: str | None = None,
    ) -> list[dict[str, Any]]:
        """Broken or gap-bearing active foreclosures that still need deed retries."""
        params: dict[str, Any] = {"retry_ttl_days": _SEARCH_NO_RESULT_RETRY_DAYS}
        sql = """
            SELECT DISTINCT f.foreclosure_id, f.case_number_raw,
                   f.case_number_norm, f.strap, f.folio,
                   COALESCE(NULLIF(btrim(f.property_address), ''), bp.property_address) AS property_address,
                   bp.owner_name,
                   bp.raw_legal1 AS legal1,
                   bp.raw_legal2 AS legal2,
                   bp.raw_legal3 AS legal3,
                   bp.raw_legal4 AS legal4
            FROM foreclosures f
            JOIN foreclosure_title_events fte
              ON fte.foreclosure_id = f.foreclosure_id
            LEFT JOIN foreclosure_title_summary ts
              ON ts.foreclosure_id = f.foreclosure_id
            LEFT JOIN hcpa_bulk_parcels bp
              ON bp.folio = f.folio
            WHERE f.archived_at IS NULL
              AND f.folio IS NOT NULL
              AND btrim(f.folio) <> ''
              AND (
                  ts.foreclosure_id IS NULL
                  OR COALESCE(ts.gap_count, 0) > 0
                  OR COALESCE(ts.chain_status, '') <> 'COMPLETE'
              )
              AND NOT EXISTS (
                  SELECT 1 FROM foreclosure_title_events e2
                  WHERE e2.foreclosure_id = f.foreclosure_id
                    AND e2.event_source = 'ORI_DEED_SEARCH'
                    AND COALESCE(e2.event_subtype, '') <> 'SEARCH_NO_RESULT'
              )
              AND NOT EXISTS (
                  SELECT 1 FROM foreclosure_title_events e2
                  WHERE e2.foreclosure_id = f.foreclosure_id
                    AND e2.event_source = 'ORI_DEED_SEARCH'
                    AND e2.event_subtype = 'SEARCH_NO_RESULT'
                    AND e2.event_date >= CURRENT_DATE - CAST(:retry_ttl_days AS INTEGER)
              )
        """
        if foreclosure_id is not None:
            sql += " AND f.foreclosure_id = :foreclosure_id"
            params["foreclosure_id"] = foreclosure_id
        if case_number:
            sql += " AND f.case_number_raw = :case_number"
            params["case_number"] = case_number
        sql += " ORDER BY f.foreclosure_id"
        if limit:
            sql += f" LIMIT {int(limit)}"

        with self.engine.connect() as conn:
            rows = conn.execute(text(sql), params).mappings().fetchall()
        return [dict(r) for r in rows]

    def _find_recent_sentinel_skips(
        self,
        *,
        foreclosure_id: int | None = None,
        case_number: str | None = None,
    ) -> list[dict[str, Any]]:
        """Foreclosures currently skipped because a recent no-result sentinel is active."""
        params: dict[str, Any] = {"retry_ttl_days": _SEARCH_NO_RESULT_RETRY_DAYS}
        sql = """
            SELECT DISTINCT
                   f.foreclosure_id,
                   f.case_number_raw,
                   f.case_number_norm,
                   f.strap,
                   f.folio,
                   COALESCE(ts.chain_status, '') AS chain_status,
                   COALESCE(ts.gap_count, 0) AS gap_count,
                   sentinel.event_date AS sentinel_date,
                   sentinel.retry_eligible_on AS retry_eligible_on,
                   GREATEST(
                       0,
                       CAST(sentinel.retry_eligible_on - CURRENT_DATE AS INTEGER)
                   ) AS retry_days_remaining
            FROM foreclosures f
            JOIN LATERAL (
                SELECT
                    e2.event_date,
                    e2.event_date + CAST(:retry_ttl_days AS INTEGER) AS retry_eligible_on
                FROM foreclosure_title_events e2
                WHERE e2.foreclosure_id = f.foreclosure_id
                  AND e2.event_source = 'ORI_DEED_SEARCH'
                  AND e2.event_subtype = 'SEARCH_NO_RESULT'
                  AND e2.event_date >= CURRENT_DATE - CAST(:retry_ttl_days AS INTEGER)
                ORDER BY e2.event_date DESC
                LIMIT 1
            ) sentinel ON TRUE
            JOIN foreclosure_title_events fte
              ON fte.foreclosure_id = f.foreclosure_id
            LEFT JOIN foreclosure_title_summary ts
              ON ts.foreclosure_id = f.foreclosure_id
            WHERE f.archived_at IS NULL
              AND f.folio IS NOT NULL
              AND btrim(f.folio) <> ''
              AND (
                  ts.foreclosure_id IS NULL
                  OR COALESCE(ts.gap_count, 0) > 0
                  OR COALESCE(ts.chain_status, '') <> 'COMPLETE'
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM foreclosure_title_events e2
                  WHERE e2.foreclosure_id = f.foreclosure_id
                    AND e2.event_source = 'ORI_DEED_SEARCH'
                    AND COALESCE(e2.event_subtype, '') <> 'SEARCH_NO_RESULT'
              )
        """
        if foreclosure_id is not None:
            sql += " AND f.foreclosure_id = :foreclosure_id"
            params["foreclosure_id"] = foreclosure_id
        if case_number:
            sql += " AND f.case_number_raw = :case_number"
            params["case_number"] = case_number
        sql += " ORDER BY sentinel.retry_eligible_on, f.foreclosure_id"

        with self.engine.connect() as conn:
            rows = conn.execute(text(sql), params).mappings().fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def _log_recent_sentinel_skips(skips: list[dict[str, Any]]) -> None:
        if not skips:
            return

        logger.info(
            "title_breaks: skipping {} foreclosures due to recent SEARCH_NO_RESULT sentinels",
            len(skips),
        )
        for skip in skips:
            logger.info(
                "title_breaks: skipping foreclosure_id={} case={} folio={} strap={} "
                "chain_status={} gap_count={} due to SEARCH_NO_RESULT sentinel dated {} "
                "(retry eligible {}, {} day(s) remaining)",
                skip.get("foreclosure_id"),
                skip.get("case_number_raw") or "",
                skip.get("folio") or "",
                skip.get("strap") or "",
                skip.get("chain_status") or "",
                int(skip.get("gap_count") or 0),
                skip.get("sentinel_date"),
                skip.get("retry_eligible_on"),
                int(skip.get("retry_days_remaining") or 0),
            )

    def _process_one(self, target: dict[str, Any]) -> tuple[int, int, int]:
        """Process a single foreclosure: find gaps, search ORI, insert deeds.

        Returns (gaps_found, deeds_inserted, sentinels_inserted).
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
            return 0, 0, 0

        all_deeds: list[dict[str, Any]] = []

        for gap in gaps:
            from_date = gap["missing_from_date"] or date(1970, 1, 1)
            import datetime as dt

            to_date = gap["missing_to_date"] or dt.datetime.now(dt.UTC).date()
            # Ensure date types (PG may return date objects already)
            if isinstance(from_date, str):
                from_date = date.fromisoformat(from_date)
            if isinstance(to_date, str):
                to_date = date.fromisoformat(to_date)

            all_deeds.extend(
                self._search_gap_deeds(
                    target,
                    gap,
                    from_date=from_date,
                    to_date=to_date,
                )
            )

        if not all_deeds:
            sentinels = self._insert_search_sentinel(target)
            return len(gaps), 0, sentinels

        inserted = self._insert_deeds(target, all_deeds)
        return len(gaps), inserted, 0

    def _search_gap_deeds(
        self,
        target: dict[str, Any],
        gap: dict[str, Any],
        *,
        from_date: date,
        to_date: date,
    ) -> list[dict[str, Any]]:
        party = gap["expected_from_party"] or gap["observed_to_party"]
        search_context = self._build_gap_search_context(target, gap)
        search_names = search_context["search_names"]
        support_names = search_context["support_names"]
        legal_terms = search_context["legal_terms"]
        if not search_names:
            return []

        original_party = (party or "").strip()
        for idx, search_name in enumerate(search_names):
            stats: dict[str, int] = {
                "api_calls": 0,
                "retries": 0,
                "truncated": 0,
                "unresolved_truncations": 0,
            }
            alias_search = idx > 0
            high_volume_builder = self._looks_high_volume_builder(search_name)

            if high_volume_builder:
                docs = self._search_gap_in_local_ori(
                    target,
                    gap,
                    party=search_name,
                    from_date=from_date,
                    to_date=to_date,
                    extra_targets=support_names,
                )
                if docs:
                    logger.info(
                        "title_breaks: local ORI builder recovery hit for search_name={!r} folio={}",
                        search_name,
                        target["folio"],
                    )
                    return docs

                docs = self._search_gap_by_legal(
                    target,
                    gap,
                    from_date=from_date,
                    to_date=to_date,
                    extra_targets=support_names,
                )
                if docs:
                    logger.info(
                        "title_breaks: legal-first builder recovery hit for search_name={!r} folio={}",
                        search_name,
                        target["folio"],
                    )
                    return docs
                continue

            try:
                party_results = self._ori.search_party_pav(
                    search_name,
                    stats,
                    from_date=from_date,
                    to_date=to_date,
                    split_on_truncated=True,
                )
            except Exception as exc:
                logger.warning(
                    "title_breaks: PAV search failed for party={!r} folio={}: {}",
                    search_name,
                    target["folio"],
                    exc,
                )
                party_results = []

            deeds = self._filter_party_search_deeds(
                party_results,
                gap,
                support_names=support_names,
                legal_terms=legal_terms,
                alias_search=alias_search,
            )
            if deeds:
                if alias_search:
                    logger.info(
                        "title_breaks: civil context alias {!r} recovered deed candidates after primary {!r} for folio={}",
                        search_name,
                        original_party,
                        target["folio"],
                    )
                time.sleep(0.5)
                return deeds

            should_try_local_fallback = alias_search or stats["unresolved_truncations"] > 0
            if should_try_local_fallback:
                docs = self._search_gap_in_local_ori(
                    target,
                    gap,
                    party=search_name,
                    from_date=from_date,
                    to_date=to_date,
                    extra_targets=support_names,
                )
                if docs:
                    logger.info(
                        "title_breaks: local ORI fallback recovered search_name={!r} folio={}",
                        search_name,
                        target["folio"],
                    )
                    return docs

                docs = self._search_gap_by_legal(
                    target,
                    gap,
                    from_date=from_date,
                    to_date=to_date,
                    extra_targets=support_names,
                )
                if docs:
                    logger.info(
                        "title_breaks: legal fallback recovered search_name={!r} folio={}",
                        search_name,
                        target["folio"],
                    )
                    return docs

            time.sleep(0.5)
        return []

    def _build_gap_search_context(
        self,
        target: dict[str, Any],
        gap: dict[str, Any],
    ) -> dict[str, Any]:
        search_names: list[str] = []
        support_names: set[str] = set()
        prioritized_aliases: list[tuple[int, str]] = []
        address_backed_names: set[str] = set()
        property_address_norm = self._normalize_address_text(target.get("property_address"))
        original_party = (gap.get("expected_from_party") or gap.get("observed_to_party") or "").strip()
        original_norm = self._normalize_party_text(original_party)
        gap_type = (gap.get("gap_type") or "").strip().lower()
        address_first = gap_type == "missing_party" and bool(property_address_norm)
        legal_terms = self._legal_search_terms(target)

        def _add_search_name(name: str | None) -> None:
            text_value = (name or "").strip()
            if not text_value:
                return
            if all(self._normalize_party_text(existing) != self._normalize_party_text(text_value) for existing in search_names):
                search_names.append(text_value)

        def _add_alias(priority: int, name: str | None) -> None:
            text_value = (name or "").strip()
            norm_value = self._normalize_party_text(text_value)
            if not norm_value:
                return
            support_names.add(norm_value)
            if norm_value == original_norm:
                return
            if any(self._normalize_party_text(existing) == norm_value for _, existing in prioritized_aliases):
                return
            prioritized_aliases.append((priority, text_value))

        if original_norm:
            support_names.add(original_norm)

        case_rows = self._load_case_party_context_rows(target, property_address_norm)
        address_case_rows = [row for row in case_rows if row.get("address_match")]
        non_address_case_rows = [row for row in case_rows if not row.get("address_match")]
        historical_rows = self._load_historical_party_context_rows(
            target=target,
            property_address_norm=property_address_norm,
        )

        if address_first:
            for row in address_case_rows:
                for alias in self._party_aliases_from_context_row(row):
                    address_backed_names.add(self._normalize_party_text(alias))
                    _add_alias(0, alias)
            for row in historical_rows:
                for alias in self._party_aliases_from_context_row(row):
                    address_backed_names.add(self._normalize_party_text(alias))
                    _add_alias(1, alias)
            if original_norm and original_norm in address_backed_names:
                _add_search_name(original_party)
            if original_party:
                _add_alias(2, original_party)
        else:
            _add_search_name(original_party)
            for row in address_case_rows:
                for alias in self._party_aliases_from_context_row(row):
                    _add_alias(1, alias)
            for row in historical_rows:
                for alias in self._party_aliases_from_context_row(row):
                    _add_alias(2, alias)

        for row in non_address_case_rows:
            for alias in self._party_aliases_from_context_row(row):
                _add_alias(4, alias)

        owner_name = (target.get("owner_name") or "").strip()
        if owner_name:
            _add_alias(5, owner_name)

        if address_first and not prioritized_aliases:
            _add_search_name(original_party)

        prioritized_aliases.sort(key=lambda item: (item[0], item[1]))
        for _, alias in prioritized_aliases[: max(0, _MAX_CONTEXT_SEARCH_NAMES - len(search_names))]:
            _add_search_name(alias)

        return {
            "search_names": search_names,
            "support_names": support_names,
            "legal_terms": legal_terms,
        }

    def _search_gap_by_legal(
        self,
        target: dict[str, Any],
        gap: dict[str, Any],
        *,
        from_date: date,
        to_date: date,
        extra_targets: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        terms = self._legal_search_terms(target)
        if not terms:
            return []

        stats: dict[str, int] = {
            "api_calls": 0,
            "retries": 0,
            "truncated": 0,
            "unresolved_truncations": 0,
        }
        matched: dict[str, dict[str, Any]] = {}
        for term in terms:
            try:
                docs = self._ori.search_legal_pav(
                    term,
                    stats,
                    from_date=from_date,
                    to_date=to_date,
                    split_on_truncated=True,
                )
            except Exception as exc:
                logger.warning(
                    "title_breaks: legal PAV search failed for term={!r} folio={}: {}",
                    term,
                    target["folio"],
                    exc,
                )
                continue

            for doc in self._deed_docs(docs):
                if not self._doc_matches_gap_parties(doc, gap, extra_targets=extra_targets):
                    continue
                instrument = (doc.get("Instrument") or "").strip()
                if instrument:
                    matched[instrument] = doc
            time.sleep(0.5)

        return list(matched.values())

    def _search_gap_in_local_ori(
        self,
        target: dict[str, Any],
        gap: dict[str, Any],
        *,
        party: str,
        from_date: date,
        to_date: date,
        extra_targets: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        legal_terms = self._legal_search_terms(target)
        like_party = f"%{party.strip()}%"
        with self.engine.connect() as conn:
            rows = (
                conn.execute(
                    text("""
                        SELECT instrument_number,
                               doc_type,
                               recording_date,
                               parties_from_text,
                               parties_to_text,
                               legal_description,
                               book_number,
                               page_number
                        FROM official_records_daily_instruments
                        WHERE recording_date BETWEEN :from_date AND :to_date
                          AND (
                              COALESCE(parties_from_text, '') ILIKE :party
                              OR COALESCE(parties_to_text, '') ILIKE :party
                          )
                          AND COALESCE(doc_type, '') ILIKE '%DEED%'
                        ORDER BY recording_date, instrument_number
                    """),
                    {
                        "from_date": from_date,
                        "to_date": to_date,
                        "party": like_party,
                    },
                )
                .mappings()
                .fetchall()
            )

        scored_docs: list[tuple[tuple[int, int, int, int], dict[str, Any]]] = []
        expected = self._normalize_party_text(gap.get("expected_from_party"))
        observed = self._normalize_party_text(gap.get("observed_to_party"))
        for row in rows:
            doc = {
                "Instrument": row["instrument_number"],
                "DocType": row["doc_type"] or "",
                "RecordDate": row["recording_date"].isoformat() if row["recording_date"] else "",
                "Book": row["book_number"] or "",
                "Page": row["page_number"] or "",
                "Legal": row["legal_description"] or "",
                "PartiesOne": self._split_party_text(row["parties_from_text"]),
                "PartiesTwo": self._split_party_text(row["parties_to_text"]),
            }
            if not self._doc_matches_gap_parties(doc, gap, extra_targets=extra_targets):
                continue
            if legal_terms and not self._doc_matches_legal_terms(doc, legal_terms):
                if row["legal_description"]:
                    continue
                if len(rows) > 3:
                    continue
            score = self._local_ori_doc_score(
                doc,
                expected=expected,
                observed=observed,
                extra_targets=extra_targets,
            )
            if score[0] <= 0:
                continue
            scored_docs.append((score, doc))

        if not scored_docs:
            return []

        scored_docs.sort(key=lambda item: item[0], reverse=True)
        best_score = scored_docs[0][0]
        deduped: dict[str, dict[str, Any]] = {}
        for score, doc in scored_docs:
            if score != best_score:
                break
            instrument = (doc.get("Instrument") or "").strip()
            if instrument:
                deduped[instrument] = doc
        return list(deduped.values())

    @staticmethod
    def _deed_docs(docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            doc
            for doc in docs
            if normalize_document_type(doc.get("DocType") or "") in _DEED_TYPES
        ]

    def _legal_search_terms(self, target: dict[str, Any]) -> list[str]:
        terms = self._ori._build_search_terms(target)  # noqa: SLF001
        return [term for term in terms if term.strip()]

    @staticmethod
    def _normalize_party_text(value: str | None) -> str:
        text_value = html.unescape(value or "").upper()
        text_value = re.sub(r"[^A-Z0-9]+", " ", text_value)
        return re.sub(r"\s+", " ", text_value).strip()

    @classmethod
    def _split_party_text(cls, value: str | None) -> list[str]:
        if not value:
            return []
        return [part.strip() for part in value.split(";") if part.strip()]

    @classmethod
    def _looks_high_volume_builder(cls, party: str | None) -> bool:
        normalized = cls._normalize_party_text(party)
        return any(token in normalized for token in _HIGH_VOLUME_BUILDER_TOKENS)

    @classmethod
    def _doc_matches_gap_parties(
        cls,
        doc: dict[str, Any],
        gap: dict[str, Any],
        *,
        extra_targets: set[str] | None = None,
    ) -> bool:
        expected = cls._normalize_party_text(gap.get("expected_from_party"))
        observed = cls._normalize_party_text(gap.get("observed_to_party"))
        targets = [value for value in (expected, observed) if value]
        if extra_targets:
            targets.extend(value for value in extra_targets if value)
        if not targets:
            return True

        doc_parties = [
            cls._normalize_party_text(name)
            for name in [
                *(doc.get("PartiesOne") or []),
                *(doc.get("PartiesTwo") or []),
            ]
            if cls._normalize_party_text(name)
        ]
        if not doc_parties:
            return False

        for target in targets:
            for doc_party in doc_parties:
                if target in doc_party or doc_party in target:
                    return True
        return False

    @classmethod
    def _doc_matches_legal_terms(
        cls,
        doc: dict[str, Any],
        legal_terms: list[str],
    ) -> bool:
        if not legal_terms:
            return True
        legal_text = cls._normalize_party_text(doc.get("Legal"))
        return any(cls._normalize_party_text(term) in legal_text for term in legal_terms if term.strip())

    @classmethod
    def _local_ori_doc_score(
        cls,
        doc: dict[str, Any],
        *,
        expected: str,
        observed: str,
        extra_targets: set[str] | None = None,
    ) -> tuple[int, int, int, int]:
        grantor_blob = cls._normalize_party_text(" ; ".join(doc.get("PartiesOne") or []))
        grantee_blob = cls._normalize_party_text(" ; ".join(doc.get("PartiesTwo") or []))

        expected_in_grantor = int(bool(expected and expected in grantor_blob))
        expected_in_grantee = int(bool(expected and expected in grantee_blob))
        observed_in_grantor = int(bool(observed and observed in grantor_blob))
        observed_in_grantee = int(bool(observed and observed in grantee_blob))

        directional_hits = expected_in_grantor + observed_in_grantee
        total_hits = (
            expected_in_grantor
            + expected_in_grantee
            + observed_in_grantor
            + observed_in_grantee
        )
        exact_side_bonus = int(expected_in_grantor and observed_in_grantee)
        if not expected and observed:
            exact_side_bonus = observed_in_grantee
        if expected and not observed:
            exact_side_bonus = expected_in_grantor
        support_hits = 0
        for support_name in extra_targets or set():
            if not support_name or support_name in {expected, observed}:
                continue
            support_hits += int(support_name in grantor_blob or support_name in grantee_blob)
        return (exact_side_bonus, directional_hits, total_hits, support_hits)

    @classmethod
    def _normalize_address_text(
        cls,
        value: str | None,
        *,
        address2: str | None = None,
        city: str | None = None,
        state: str | None = None,
        postal_code: str | None = None,
    ) -> str:
        address1_value = html.unescape(value or "").strip()
        if not address1_value:
            return ""

        address2_value = html.unescape(address2 or "").strip() or None
        city_value = html.unescape(city or "").strip() or None
        state_value = html.unescape(state or "").strip() or None
        postal_code_value = html.unescape(postal_code or "").strip() or None

        try:
            normalized = normalize_address_record(
                {
                    "address_line_1": address1_value,
                    "address_line_2": address2_value,
                    "city": city_value,
                    "state": state_value,
                    "postal_code": postal_code_value,
                }
                if any((address2_value, city_value, state_value, postal_code_value))
                else address1_value
            )
            normalized_line1 = (normalized.get("address_line_1") or "").strip()
            normalized_line2 = (normalized.get("address_line_2") or "").strip()
            normalized_text = " ".join(
                part for part in (normalized_line1, normalized_line2) if part
            )
            if normalized_text:
                return normalized_text.upper()
        except Exception as exc:
            logger.debug(
                "title_breaks: scourgify normalization failed for address={!r}: {}",
                address1_value,
                exc,
            )

        return cls._fallback_normalize_address_text(
            " ".join(part for part in (address1_value, address2_value or "") if part)
        )

    @classmethod
    def _fallback_normalize_address_text(cls, value: str | None) -> str:
        text_value = html.unescape(value or "").upper()
        text_value = text_value.split(",", 1)[0].strip()
        if not text_value:
            return ""
        replacements = {
            " STREET": " ST",
            " AVENUE": " AVE",
            " BOULEVARD": " BLVD",
            " DRIVE": " DR",
            " ROAD": " RD",
            " LANE": " LN",
            " COURT": " CT",
            " CIRCLE": " CIR",
            " PLACE": " PL",
            " NORTH": " N",
            " SOUTH": " S",
            " EAST": " E",
            " WEST": " W",
            " NORTHEAST": " NE",
            " NORTHWEST": " NW",
            " SOUTHEAST": " SE",
            " SOUTHWEST": " SW",
        }
        for full, abbr in replacements.items():
            text_value = text_value.replace(full, abbr)
        text_value = re.sub(r"[^A-Z0-9# ]+", " ", text_value)
        return re.sub(r"\s+", " ", text_value).strip()

    @classmethod
    def _address_search_terms(cls, value: str | None) -> tuple[str | None, list[str]]:
        normalized = cls._normalize_address_text(value)
        if not normalized:
            return None, []

        tokens = [token for token in re.findall(r"[A-Z0-9#]+", normalized) if token]
        if not tokens:
            return None, []

        house_number = tokens[0] if tokens[0][0].isdigit() else None
        if not house_number:
            return None, []

        street_tokens: list[str] = []
        for token in tokens[1:]:
            if token in _ADDRESS_UNIT_TOKENS:
                break
            if token in _ADDRESS_SUFFIX_TOKENS:
                break
            if token in _ADDRESS_DIRECTION_TOKENS:
                if street_tokens:
                    street_tokens.append(token)
                continue
            street_tokens.append(token)

        if not street_tokens:
            street_tokens = [
                token
                for token in tokens[1:]
                if token not in _ADDRESS_UNIT_TOKENS
            ][:1]

        return f"{house_number}%", street_tokens[:2]

    @classmethod
    def _split_aka_text(cls, value: str | None) -> list[str]:
        text_value = html.unescape(value or "").strip()
        if not text_value:
            return []
        return [
            part.strip()
            for part in re.split(r"\s*[;|/]\s*", text_value)
            if part.strip()
        ]

    @classmethod
    def _party_aliases_from_context_row(cls, row: dict[str, Any]) -> list[str]:
        aliases: list[str] = []
        for value in [
            row.get("name"),
            row.get("business_name"),
            *cls._split_aka_text(row.get("akas")),
        ]:
            text_value = (value or "").strip()
            if not text_value:
                continue
            if all(cls._normalize_party_text(existing) != cls._normalize_party_text(text_value) for existing in aliases):
                aliases.append(text_value)
        return aliases

    def _load_case_party_context_rows(
        self,
        target: dict[str, Any],
        property_address_norm: str,
    ) -> list[dict[str, Any]]:
        case_number_norm = (target.get("case_number_norm") or "").strip()
        case_number_raw = (target.get("case_number_raw") or "").strip()
        cache_key = (case_number_norm, case_number_raw)
        cache = getattr(self, "_case_party_context_cache", None)
        if cache is None:
            cache = {}
            self._case_party_context_cache = cache
        if cache_key in cache:
            return cache[cache_key]

        if not case_number_norm and not case_number_raw:
            cache[cache_key] = []
            return []

        sql = text("""
            SELECT
                c.case_number,
                c.filing_date,
                p.party_type,
                p.name,
                p.business_name,
                p.akas,
                p.address1,
                p.address2,
                p.city,
                p.state,
                p.zip
            FROM clerk_civil_parties p
            JOIN clerk_civil_cases c
              ON c.case_number = p.case_number
            WHERE (
                    (:case_number_norm <> '' AND c.case_number = :case_number_norm)
                 OR (:case_number_raw <> '' AND c.case_number = :case_number_raw)
            )
              AND COALESCE(p.party_type, '') NOT ILIKE 'Plaintiff%'
              AND NULLIF(btrim(COALESCE(p.name, p.business_name, '')), '') IS NOT NULL
            ORDER BY
                CASE
                    WHEN NULLIF(btrim(COALESCE(p.address1, '')), '') IS NOT NULL THEN 0
                    ELSE 1
                END,
                c.filing_date DESC NULLS LAST,
                p.party_type,
                p.name
        """)
        with self.engine.connect() as conn:
            rows = conn.execute(
                sql,
                {
                    "case_number_norm": case_number_norm,
                    "case_number_raw": case_number_raw,
                },
            ).mappings().fetchall()

        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["address_match"] = (
                bool(property_address_norm)
                and self._normalize_address_text(
                    item.get("address1"),
                    address2=item.get("address2"),
                    city=item.get("city"),
                    state=item.get("state"),
                    postal_code=item.get("zip"),
                )
                == property_address_norm
            )
            result.append(item)
        cache[cache_key] = result
        return result

    def _load_historical_party_context_rows(
        self,
        *,
        target: dict[str, Any],
        property_address_norm: str,
    ) -> list[dict[str, Any]]:
        if not property_address_norm:
            return []

        house_prefix, street_tokens = self._address_search_terms(property_address_norm)
        if not house_prefix:
            return []

        case_number_norm = (target.get("case_number_norm") or "").strip()
        case_number_raw = (target.get("case_number_raw") or "").strip()
        cache_key = (property_address_norm, case_number_norm, case_number_raw)
        cache = getattr(self, "_historical_party_context_cache", None)
        if cache is None:
            cache = {}
            self._historical_party_context_cache = cache
        if cache_key in cache:
            return cache[cache_key]

        where_clauses = [
            "c.is_foreclosure IS TRUE",
            "(:case_number_norm = '' OR c.case_number <> :case_number_norm)",
            "(:case_number_raw = '' OR c.case_number <> :case_number_raw)",
            "COALESCE(p.party_type, '') NOT ILIKE 'Plaintiff%'",
            "NULLIF(btrim(COALESCE(p.address1, '')), '') IS NOT NULL",
            "UPPER(COALESCE(p.address1, '')) LIKE :house_prefix",
            "NULLIF(btrim(COALESCE(p.name, p.business_name, '')), '') IS NOT NULL",
        ]
        params: dict[str, Any] = {
            "case_number_norm": case_number_norm,
            "case_number_raw": case_number_raw,
            "house_prefix": house_prefix,
            "row_limit": _HISTORICAL_CONTEXT_LIMIT,
        }
        for idx, token in enumerate(street_tokens[:2]):
            where_clauses.append(
                f"UPPER(COALESCE(p.address1, '')) LIKE :street_token_{idx}"
            )
            params[f"street_token_{idx}"] = f"%{token}%"
        sql_text = f"""
            SELECT
                c.case_number,
                c.filing_date,
                p.party_type,
                p.name,
                p.business_name,
                p.akas,
                p.address1,
                p.address2,
                p.city,
                p.state,
                p.zip
            FROM clerk_civil_parties p
            JOIN clerk_civil_cases c
              ON c.case_number = p.case_number
            WHERE {' AND '.join(where_clauses)}
            ORDER BY
                CASE
                    WHEN COALESCE(p.party_type, '') ILIKE 'Defendant%' THEN 0
                    ELSE 1
                END,
                c.filing_date DESC NULLS LAST,
                c.case_number
            LIMIT :row_limit
        """

        sql = text(sql_text)
        with self.engine.connect() as conn:
            rows = conn.execute(
                sql,
                params,
            ).mappings().fetchall()

        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["address_match"] = (
                self._normalize_address_text(
                    item.get("address1"),
                    address2=item.get("address2"),
                    city=item.get("city"),
                    state=item.get("state"),
                    postal_code=item.get("zip"),
                )
                == property_address_norm
            )
            if not item["address_match"]:
                continue
            result.append(item)
        cache[cache_key] = result
        return result

    def _filter_party_search_deeds(
        self,
        docs: list[dict[str, Any]],
        gap: dict[str, Any],
        *,
        support_names: set[str],
        legal_terms: list[str],
        alias_search: bool,
    ) -> list[dict[str, Any]]:
        matched: list[dict[str, Any]] = []
        seen_instruments: set[str] = set()
        for doc in self._deed_docs(docs):
            if not self._doc_matches_gap_parties(doc, gap, extra_targets=support_names):
                continue
            if alias_search and legal_terms and not self._doc_matches_legal_terms(doc, legal_terms):
                continue
            instrument = (doc.get("Instrument") or "").strip()
            if instrument and instrument in seen_instruments:
                continue
            if instrument:
                seen_instruments.add(instrument)
            matched.append(doc)
        return matched

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
                        :event_date, :event_source, :event_subtype,
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

    def _insert_search_sentinel(self, target: dict[str, Any]) -> int:
        """Record that ORI deed search completed with no results for this foreclosure."""
        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO foreclosure_title_events (
                        foreclosure_id,
                        case_number_raw,
                        case_number_norm,
                        folio,
                        strap,
                        event_date,
                        event_source,
                        event_subtype,
                        instrument_number,
                        grantor,
                        grantee,
                        description
                    )
                    SELECT
                        :foreclosure_id,
                        :case_number_raw,
                        :case_number_norm,
                        :folio,
                        :strap,
                        :event_date,
                        'ORI_DEED_SEARCH',
                        'SEARCH_NO_RESULT',
                        NULL,
                        NULL,
                        NULL,
                        :description
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM foreclosure_title_events
                        WHERE foreclosure_id = :foreclosure_id
                          AND event_source = 'ORI_DEED_SEARCH'
                          AND COALESCE(event_subtype, '') <> 'SEARCH_NO_RESULT'
                    )
                      AND NOT EXISTS (
                        SELECT 1
                        FROM foreclosure_title_events
                        WHERE foreclosure_id = :foreclosure_id
                          AND event_source = 'ORI_DEED_SEARCH'
                          AND event_subtype = 'SEARCH_NO_RESULT'
                          AND event_date >= CURRENT_DATE - CAST(:retry_ttl_days AS INTEGER)
                    )
                """),
                {
                    "foreclosure_id": target["foreclosure_id"],
                    "case_number_raw": target["case_number_raw"],
                    "case_number_norm": target.get("case_number_norm"),
                    "folio": target["folio"],
                    "strap": target.get("strap"),
                    "event_date": dt.datetime.now(dt.UTC).date(),
                    "description": "ORI deed search completed with no matching deeds",
                    "retry_ttl_days": _SEARCH_NO_RESULT_RETRY_DAYS,
                },
            )
        return int(result.rowcount or 0)

    def _backfill_deed_parties(
        self,
        limit: int | None = None,
        *,
        foreclosure_id: int | None = None,
        case_number: str | None = None,
    ) -> int:
        """Fetch missing grantor/grantee from PAV for active-foreclosure deeds.

        Queries hcpa_allsales rows tied to active foreclosures that have a
        doc_num but NULL grantor/grantee. It hits the PAV instrument search API
        and inserts results into foreclosure_title_events so fn_title_chain
        can resolve party names.
        """
        params: dict[str, Any] = {}
        # First, find foreclosure folks missing a deed party...
        sql = """
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
                """
        if foreclosure_id is not None:
            sql += " AND f.foreclosure_id = :foreclosure_id"
            params["foreclosure_id"] = foreclosure_id
        if case_number:
            sql += " AND f.case_number_raw = :case_number"
            params["case_number"] = case_number
        sql += """
                    GROUP BY s.doc_num
                    ORDER BY MAX(s.sale_date) DESC NULLS LAST
                """
        with self.engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()

        if not rows:
            return 0

        success = 0
        targets = rows[:limit] if limit else rows

        for row in targets:
            doc_num = (row.doc_num or "").strip()
            if not doc_num:
                continue

            parsed = self._lookup_instrument_parties(doc_num)

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
                            :folio, :strap, :event_date, :event_source, :event_subtype,
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

    def _lookup_instrument_parties(self, instrument: str) -> dict[str, str | None] | None:
        stats = {"api_calls": 0, "retries": 0}
        payload = {
            "QueryID": 320,
            "Keywords": [{"Id": 1006, "Value": instrument}],
            "QueryLimit": 5,
        }
        data = self._ori._post_pav(  # noqa: SLF001
            payload,
            f"title_break_instrument:{instrument}",
            stats,
            bypass_cache=True,
        )
        if data is None:
            logger.warning(
                "title_breaks: instrument lookup returned no data for {}",
                instrument,
            )
            return None

        rows = data.get("Data") or []
        if not rows:
            return None
        return self._parse_pav_parties(rows, instrument)

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
