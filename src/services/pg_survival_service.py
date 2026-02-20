"""Phase B Step 4: Lien survival analysis → PG ori_encumbrances.

Reads encumbrances from PG ``ori_encumbrances``, judgment data from
``foreclosures.judgment_data``, and chain-of-title from
``foreclosure_title_chain``.  Runs ``SurvivalService.analyze()`` and
writes ``survival_status`` / ``survival_reason`` back to PG.
"""

from __future__ import annotations

import json
from typing import Any

from loguru import logger
from sqlalchemy import text

from sunbiz.db import get_engine, resolve_pg_dsn


class PgSurvivalService:
    """Run lien survival analysis on PG encumbrances."""

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = resolve_pg_dsn(dsn)
        self.engine = get_engine(self.dsn)

    def run(self, *, limit: int | None = None) -> dict[str, Any]:
        """Analyze survival for foreclosures with unanalyzed encumbrances."""
        targets = self._find_targets(limit)
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
                self._save_survival_results(strap, result)
                self._mark_analyzed(fid)
                analyzed += 1

                survived = len(result["results"]["survived"])
                extinguished = len(result["results"]["extinguished"])
                logger.info(
                    f"Survival for {case}: "
                    f"{survived} survived, {extinguished} extinguished"
                )

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

    def _find_targets(self, limit: int | None) -> list[dict[str, Any]]:
        """Find foreclosures needing survival analysis."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT f.foreclosure_id, f.case_number_raw, f.strap,
                           f.judgment_data, f.homestead_exempt
                    FROM foreclosures f
                    WHERE f.step_survival_analyzed IS NULL
                      AND f.step_ori_searched IS NOT NULL
                      AND f.archived_at IS NULL
                      AND f.strap IS NOT NULL
                      AND EXISTS (
                          SELECT 1 FROM ori_encumbrances oe
                          WHERE oe.strap = f.strap
                            AND oe.survival_status IS NULL
                      )
                    ORDER BY f.auction_date
                    LIMIT :limit
                """),
                {"limit": limit or 1000},
            ).fetchall()

        targets = []
        for r in rows:
            jdata = r[3] or {}
            if isinstance(jdata, str):
                try:
                    jdata = json.loads(jdata)
                except (json.JSONDecodeError, TypeError):
                    jdata = {}

            targets.append({
                "foreclosure_id": r[0],
                "case_number": r[1],
                "strap": r[2],
                "judgment_data": jdata,
                "homestead_exempt": bool(r[4]),
            })

        return targets

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def _load_encumbrances(self, strap: str) -> list[dict[str, Any]]:
        """Load encumbrances from PG ori_encumbrances as dicts for SurvivalService."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT id, encumbrance_type, party1, party2,
                           amount, recording_date, instrument_number,
                           book, page, is_satisfied,
                           satisfaction_instrument, satisfaction_date,
                           survival_status, case_number
                    FROM ori_encumbrances
                    WHERE strap = :strap
                    ORDER BY recording_date NULLS LAST
                """),
                {"strap": strap},
            ).fetchall()

        encumbrances = []
        for r in rows:
            encumbrances.append({
                "id": r[0],
                "encumbrance_type": r[1] or "other",
                "creditor": r[2] or "",
                "debtor": r[3] or "",
                "amount": float(r[4]) if r[4] else 0.0,
                "recording_date": str(r[5]) if r[5] else None,
                "instrument": r[6] or "",
                "book": r[7] or "",
                "page": r[8] or "",
                "is_satisfied": bool(r[9]),
                "satisfaction_instrument": r[10] or "",
                "satisfaction_date": str(r[11]) if r[11] else None,
                "survival_status": r[12],
                "case_number": r[13] or "",
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
        strap: str,
        result: dict[str, Any],
    ) -> None:
        """Write survival_status + survival_reason back to PG ori_encumbrances."""
        all_encs = []
        for category in (
            "survived", "extinguished", "expired", "satisfied",
            "historical", "foreclosing", "uncertain",
        ):
            all_encs.extend(result["results"].get(category, []))

        if not all_encs:
            return

        with self.engine.begin() as conn:
            for enc in all_encs:
                enc_id = enc.get("id")
                status = enc.get("survival_status")
                reason = enc.get("survival_reason")

                if not enc_id or not status:
                    continue

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
                        "case_number": enc.get("case_number"),
                    },
                )

    def _mark_analyzed(self, foreclosure_id: int) -> None:
        """Mark foreclosure as survival-analyzed."""
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE foreclosures SET step_survival_analyzed = now() "
                    "WHERE foreclosure_id = :fid"
                ),
                {"fid": foreclosure_id},
            )
