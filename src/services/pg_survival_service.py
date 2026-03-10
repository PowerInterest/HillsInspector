"""Phase B Step 4: Lien survival analysis with per-foreclosure persistence.

Reads encumbrances from PG ``ori_encumbrances``, judgment data from
``foreclosures.judgment_data``, and chain-of-title from
``foreclosure_title_chain``. Runs ``SurvivalService.analyze()`` and persists
results to ``foreclosure_encumbrance_survival`` keyed by
``(foreclosure_id, encumbrance_id)``.

That extra scope is required because two active foreclosure cases can share the
same parcel/strap. A single parcel-scoped ``ori_encumbrances.survival_status``
column cannot represent both cases correctly. The service still updates the
legacy row-level status columns for compatibility, but correctness-sensitive
reads should prefer ``foreclosure_encumbrance_survival`` and fall back to the
legacy columns only when no per-foreclosure row exists.

NOCs (encumbrance_type='noc') are excluded from both target selection and
encumbrance loading — they are administrative notices, not liens.
See docs/NOC_PERMIT_LINKING.md for the full exclusion map and
docs/domain/PER_FORECLOSURE_SURVIVAL.md for the persistence model.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from loguru import logger
from sqlalchemy import text

from sunbiz.db import get_engine, resolve_pg_dsn

if TYPE_CHECKING:
    from collections.abc import Sequence


class PgSurvivalService:
    """Run lien survival analysis on PG encumbrances."""

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = resolve_pg_dsn(dsn)
        self.engine = get_engine(self.dsn)

    @staticmethod
    def _encumbrance_parties(
        encumbrance_type: str,
        party1: str,
        party2: str,
        current_holder: str,
    ) -> tuple[str, str]:
        """Map ORI party columns into creditor/debtor semantics for survival."""
        enc_type = (encumbrance_type or "").lower()
        if enc_type == "mortgage":
            creditor = current_holder or party2 or party1
            debtor = party1 or party2
            return creditor, debtor
        return party1 or current_holder or party2, party2 or party1

    def run(
        self,
        *,
        limit: int | None = None,
        foreclosure_ids: Sequence[int] | None = None,
        force_reanalysis: bool = False,
    ) -> dict[str, Any]:
        """Analyze survival for foreclosures with unanalyzed encumbrances."""
        parsed_ids: set[int] = set()
        for foreclosure_id in foreclosure_ids or []:
            try:
                fid = int(foreclosure_id)
            except (TypeError, ValueError):
                continue
            if fid > 0:
                parsed_ids.add(fid)
        target_ids = sorted(parsed_ids)
        if foreclosure_ids is not None and not target_ids:
            return {"skipped": True, "reason": "no_target_foreclosures"}

        targets = self._find_targets(
            limit,
            foreclosure_ids=target_ids or None,
            force_reanalysis=force_reanalysis,
        )
        if not targets:
            return {"skipped": True, "reason": "no_foreclosures_need_survival"}

        logger.info(f"Survival analysis: {len(targets)} foreclosures")

        from src.services.lien_survival.survival_service import SurvivalService

        analyzed = 0
        errors = 0

        for target in targets:
            fid = target["foreclosure_id"]
            strap = target["strap"]
            case = target["case_number"]

            try:
                # Load encumbrances from PG
                encumbrances = self._load_encumbrances(strap)
                if not encumbrances:
                    self._mark_analyzed(fid)
                    continue

                # Load judgment data
                jdata = target.get("judgment_data") or {}

                # Load chain of title from PG
                chain = self._load_chain(fid)
                current_period_id = chain[-1]["id"] if chain else None

                # Check homestead
                is_homestead = target.get("homestead_exempt", False)

                # Run survival analysis
                svc = SurvivalService(property_id=strap)
                result = svc.analyze(
                    encumbrances=encumbrances,
                    judgment_data=jdata,
                    chain_of_title=chain,
                    current_period_id=current_period_id,
                    is_homestead=is_homestead,
                )

                # Write results back to PG
                self._save_survival_results(fid, case, strap, result)
                self._mark_analyzed(fid)
                analyzed += 1

                survived = len(result["results"]["survived"])
                extinguished = len(result["results"]["extinguished"])
                logger.info(f"Survival for {case}: {survived} survived, {extinguished} extinguished")

            except Exception as exc:
                logger.error(f"Survival analysis error for {case}: {exc}")
                errors += 1

        return {
            "targets": len(targets),
            "analyzed": analyzed,
            "errors": errors,
        }

    # ------------------------------------------------------------------
    # Target selection
    # ------------------------------------------------------------------

    def _find_targets(
        self,
        limit: int | None,
        *,
        foreclosure_ids: Sequence[int] | None = None,
        force_reanalysis: bool = False,
    ) -> list[dict[str, Any]]:
        """Find foreclosures needing survival analysis."""
        where_clauses = [
            "f.step_ori_searched IS NOT NULL",
            "f.archived_at IS NULL",
            "f.strap IS NOT NULL",
        ]
        if not force_reanalysis:
            where_clauses.insert(0, "f.step_survival_analyzed IS NULL")
        if foreclosure_ids:
            where_clauses.append("f.foreclosure_id = ANY(:foreclosure_ids)")

        exists_clauses = [
            "oe.strap = f.strap",
            "oe.encumbrance_type != 'noc'",
        ]
        if not force_reanalysis:
            exists_clauses.insert(
                1,
                """NOT EXISTS (
                    SELECT 1
                    FROM foreclosure_encumbrance_survival fes
                    WHERE fes.foreclosure_id = f.foreclosure_id
                      AND fes.encumbrance_id = oe.id
                )""",
            )

        query = f"""
            SELECT f.foreclosure_id, f.case_number_raw, f.strap,
                   f.judgment_data,
                   COALESCE(dn.homestead_exempt, f.homestead_exempt)
                      AS homestead_exempt
            FROM foreclosures f
            LEFT JOIN LATERAL (
                SELECT dn2.homestead_exempt
                FROM dor_nal_parcels dn2
                WHERE dn2.strap = f.strap
                ORDER BY dn2.tax_year DESC
                LIMIT 1
            ) dn ON true
            WHERE {" AND ".join(where_clauses)}
              AND EXISTS (
                  SELECT 1 FROM ori_encumbrances oe
                  WHERE {" AND ".join(exists_clauses)}
              )
            ORDER BY f.auction_date
            LIMIT :limit
        """
        params: dict[str, Any] = {"limit": limit or 1000}
        if foreclosure_ids:
            params["foreclosure_ids"] = list(foreclosure_ids)

        with self.engine.connect() as conn:
            rows = conn.execute(
                text(query),
                params,
            ).fetchall()

        targets = []
        for r in rows:
            jdata = r[3] or {}
            if isinstance(jdata, str):
                try:
                    jdata = json.loads(jdata)
                except (json.JSONDecodeError, TypeError):
                    jdata = {}

            # Build foreclosing_refs from foreclosed_mortgage for exact
            # lien matching.  The vision API stores instrument / book / page
            # under "foreclosed_mortgage" but the survival engine expects a
            # flat "foreclosing_refs" dict.  This mapping was present in the
            # legacy SQLite orchestrator but was dropped in the PG migration.
            if not jdata.get("foreclosing_refs") and "foreclosed_mortgage" in jdata:
                fm = jdata.get("foreclosed_mortgage")
                if fm and isinstance(fm, dict):
                    refs = {
                        "instrument": fm.get("instrument_number"),
                        "book": fm.get("recording_book") or fm.get("book"),
                        "page": fm.get("recording_page") or fm.get("page"),
                    }
                    if any(refs.values()):
                        jdata["foreclosing_refs"] = refs
            if not jdata.get("case_number") and r[1]:
                jdata["case_number"] = r[1]

            targets.append({
                "foreclosure_id": r[0],
                "case_number": r[1],
                "strap": r[2],
                "judgment_data": jdata,
                "homestead_exempt": bool(r[4]) if r[4] is not None else False,
            })

        return targets

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def _load_encumbrances(self, strap: str) -> list[dict[str, Any]]:
        """Load encumbrances from PG ori_encumbrances as dicts for SurvivalService."""
        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT id, encumbrance_type, party1, party2,
                           amount, recording_date, instrument_number,
                           book, page, is_satisfied,
                           satisfaction_instrument, satisfaction_date,
                           survival_status, case_number, current_holder
                    FROM ori_encumbrances
                    WHERE strap = :strap
                      AND encumbrance_type != 'noc'
                    ORDER BY recording_date NULLS LAST
                """),
                {"strap": strap},
            )
            rows = result.mappings().fetchall()

        encumbrances = []
        for r in rows:
            creditor, debtor = self._encumbrance_parties(
                str(r["encumbrance_type"] or ""),
                str(r["party1"] or ""),
                str(r["party2"] or ""),
                str(r["current_holder"] or ""),
            )
            encumbrances.append({
                "id": r["id"],
                "encumbrance_type": r["encumbrance_type"] or "other",
                "creditor": creditor,
                "debtor": debtor,
                "amount": float(r["amount"]) if r["amount"] else 0.0,
                "recording_date": str(r["recording_date"]) if r["recording_date"] else None,
                "instrument": r["instrument_number"] or "",
                "book": r["book"] or "",
                "page": r["page"] or "",
                "is_satisfied": bool(r["is_satisfied"]),
                "satisfaction_instrument": r["satisfaction_instrument"] or "",
                "satisfaction_date": str(r["satisfaction_date"]) if r["satisfaction_date"] else None,
                "survival_status": r["survival_status"],
                "case_number": r["case_number"] or "",
            })

        return encumbrances

    def _load_chain(self, foreclosure_id: int) -> list[dict[str, Any]]:
        """Load title chain from PG foreclosure_title_chain."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT chain_id, owner_name, acquired_date, disposed_date,
                           grantor, grantee, link_status
                    FROM foreclosure_title_chain
                    WHERE foreclosure_id = :fid
                    ORDER BY sequence_no
                """),
                {"fid": foreclosure_id},
            ).fetchall()

        chain = []
        for r in rows:
            chain.append({
                "id": r[0],
                "owner_name": r[1] or "",
                "acquisition_date": str(r[2]) if r[2] else None,
                "disposition_date": str(r[3]) if r[3] else None,
                "acquired_from": r[4] or "",
                "link_status": r[6] or "unknown",
            })

        return chain

    # ------------------------------------------------------------------
    # Result saving
    # ------------------------------------------------------------------

    def _save_survival_results(
        self,
        foreclosure_id: int,
        foreclosure_case_number: str,
        strap: str,
        result: dict[str, Any],
    ) -> None:
        """Persist survival results per foreclosure and update legacy row fields."""
        all_encs = []
        for category in (
            "survived",
            "extinguished",
            "expired",
            "satisfied",
            "historical",
            "foreclosing",
            "uncertain",
        ):
            all_encs.extend(result["results"].get(category, []))

        if not all_encs:
            return

        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    DELETE FROM foreclosure_encumbrance_survival
                    WHERE foreclosure_id = :foreclosure_id
                """),
                {"foreclosure_id": foreclosure_id},
            )
            for enc in all_encs:
                enc_id = enc.get("id")
                status = enc.get("survival_status")
                reason = enc.get("survival_reason")

                if not enc_id or not status:
                    continue

                conn.execute(
                    text("""
                        INSERT INTO foreclosure_encumbrance_survival (
                            foreclosure_id,
                            encumbrance_id,
                            survival_status,
                            survival_reason,
                            survival_case_number,
                            analyzed_at,
                            updated_at
                        ) VALUES (
                            :foreclosure_id,
                            :encumbrance_id,
                            :status,
                            :reason,
                            :case_number,
                            now(),
                            now()
                        )
                        ON CONFLICT (foreclosure_id, encumbrance_id) DO UPDATE SET
                            survival_status = EXCLUDED.survival_status,
                            survival_reason = EXCLUDED.survival_reason,
                            survival_case_number = EXCLUDED.survival_case_number,
                            analyzed_at = EXCLUDED.analyzed_at,
                            updated_at = EXCLUDED.updated_at
                    """),
                    {
                        "foreclosure_id": foreclosure_id,
                        "encumbrance_id": enc_id,
                        "status": status,
                        "reason": reason,
                        "case_number": foreclosure_case_number,
                    },
                )

                conn.execute(
                    text("""
                        UPDATE ori_encumbrances SET
                            survival_status = :status,
                            survival_reason = :reason,
                            survival_analyzed_at = now(),
                            survival_case_number = :case_number
                        WHERE id = :id
                    """),
                    {
                        "id": enc_id,
                        "status": status,
                        "reason": reason,
                        "case_number": foreclosure_case_number,
                    },
                )

    def _mark_analyzed(self, foreclosure_id: int) -> None:
        """Mark foreclosure as survival-analyzed."""
        with self.engine.begin() as conn:
            conn.execute(
                text("UPDATE foreclosures SET step_survival_analyzed = now() WHERE foreclosure_id = :fid"),
                {"fid": foreclosure_id},
            )
