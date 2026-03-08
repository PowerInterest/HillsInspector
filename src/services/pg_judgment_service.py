"""Phase B Step 2: Extract judgment data from PDFs → PG foreclosures.judgment_data.

Finds foreclosures that have a PDF on disk but no extracted JSON, runs
VisionService extraction via FinalJudgmentProcessor, then pushes the JSON
cache into PG via the refresh path.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from loguru import logger
from sqlalchemy import text

from sunbiz.db import get_engine, resolve_pg_dsn

FORECLOSURE_DATA_DIR = Path("data/Foreclosure")


class PgJudgmentService:
    """Process judgment PDFs and push extracted data to PG."""

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = resolve_pg_dsn(dsn)
        self.engine = get_engine(self.dsn)

    def run(self, *, limit: int | None = None) -> dict[str, Any]:
        """Find unprocessed PDFs, extract via Vision, push to PG."""
        # Step 1: Find foreclosures needing judgment extraction
        needs_extract = self._find_unextracted_pdfs(limit)
        if not needs_extract:
            return {"skipped": True, "reason": "all_judgments_extracted"}

        logger.info(f"Found {len(needs_extract)} PDFs needing judgment extraction")

        # Step 2: Process each PDF with FinalJudgmentProcessor
        extracted = self._extract_judgments(needs_extract)

        # Step 3: Push all JSON caches to PG (idempotent)
        loaded = self._load_judgment_data_to_pg()

        return {
            "pdfs_found": len(needs_extract),
            "pdfs_extracted": extracted,
            "judgments_loaded_to_pg": loaded,
        }

    def _find_unextracted_pdfs(self, limit: int | None) -> list[dict[str, Any]]:
        """Find PDFs on disk that don't have a corresponding _extracted.json."""
        if not FORECLOSURE_DATA_DIR.exists():
            return []

        results: list[dict[str, Any]] = []

        for case_dir in sorted(FORECLOSURE_DATA_DIR.iterdir()):
            if not case_dir.is_dir():
                continue
            doc_dir = case_dir / "documents"
            if not doc_dir.is_dir():
                continue

            # Find PDF files
            pdfs = list(doc_dir.glob("*.pdf"))
            if not pdfs:
                continue

            # Only judgment PDFs belong in this step. Mortgage PDFs live in the
            # same folder and are handled by the mortgage extraction service.
            for pdf in pdfs:
                if not pdf.stem.startswith("final_judgment_"):
                    continue
                json_cache = doc_dir / f"{pdf.stem}_extracted.json"
                if json_cache.exists():
                    continue

                results.append({
                    "case_number": case_dir.name,
                    "pdf_path": str(pdf),
                })

                if limit and len(results) >= limit:
                    break

            if limit and len(results) >= limit:
                break

        return results

    def _extract_judgments(self, items: list[dict[str, Any]]) -> int:
        """Run FinalJudgmentProcessor on each PDF."""
        from src.services.final_judgment_processor import FinalJudgmentProcessor

        processor = FinalJudgmentProcessor()

        extracted = 0
        for item in items:
            pdf_path = item["pdf_path"]
            case_number = item["case_number"]
            try:
                result = processor.process_pdf(
                    pdf_path=pdf_path,
                    case_number=case_number,
                )
                if result:
                    extracted += 1
                    logger.info(
                        f"Extracted judgment for {case_number} "
                        f"(pdf={pdf_path}): "
                        f"plaintiff={result.get('plaintiff', '?')}"
                    )
            except Exception as exc:
                logger.error(f"Judgment extraction failed for {case_number}: {exc}")

        return extracted

    @staticmethod
    def _select_best_judgment(
        json_paths: list[Path],
    ) -> tuple[Path, dict[str, Any]] | None:
        """Pick the single best final-judgment JSON from a case directory.

        Selection rules (in priority order):
        1. Only consider ``final_judgment_*_extracted.json`` files — skip
           mortgage extractions, unknown docs, etc.
        2. Among those, prefer a *non-thin* extraction (has legal_description
           or mortgage recording refs) over a thin one (likely a fee order).
        3. If there are ties, prefer the candidate with the latest extracted
           recording date and then the highest extracted instrument number.
           Filename stem is only a final fallback because storage sometimes
           falls back to case numbers instead of instruments.

        Returns ``(json_path, parsed_dict)`` or ``None`` when no suitable
        candidate exists.
        """
        from src.services.final_judgment_processor import FinalJudgmentProcessor

        candidates: list[tuple[Path, dict[str, Any], bool]] = []
        for jp in json_paths:
            # Only consider final-judgment extractions
            if not jp.name.startswith("final_judgment_"):
                continue
            try:
                jd = json.loads(jp.read_text())
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning(f"Skipping invalid judgment JSON {jp}: {exc}")
                continue
            thin = FinalJudgmentProcessor.is_thin_extraction(jd)
            candidates.append((jp, jd, thin))

        if not candidates:
            return None

        def _rank(item: tuple[Path, dict[str, Any], bool]) -> tuple[int, str, int, str]:
            path, judgment_data, thin = item
            recording_date = str(judgment_data.get("recording_date") or "").strip()
            instrument = str(judgment_data.get("instrument_number") or "").strip()
            stem_digits = re.sub(r"\D", "", path.stem)
            instrument_digits = re.sub(r"\D", "", instrument)
            instrument_rank = int(instrument_digits or stem_digits or "0")
            return (
                1 if not thin else 0,
                recording_date,
                instrument_rank,
                path.stem,
            )

        best = max(candidates, key=_rank)

        return best[0], best[1]

    def _load_judgment_data_to_pg(self) -> int:
        """Scan final-judgment extracted JSONs and push to PG foreclosures.

        For each case directory we pick the single best final-judgment JSON
        (see ``_select_best_judgment``) and derive the matching PDF path from
        its stem (``{stem}_extracted.json`` -> ``{stem}.pdf``).  This avoids
        the previous bug where every ``*_extracted.json`` (including mortgage
        extractions) overwrote the same foreclosure row, with the PDF path
        pointing at whichever ``*.pdf`` the filesystem happened to yield first.
        """
        if not FORECLOSURE_DATA_DIR.exists():
            return 0

        with self.engine.begin() as conn:
            # Build lookup maps
            rows = conn.execute(
                text(
                    "SELECT DISTINCT ON (case_number_raw) foreclosure_id, case_number_raw, strap "
                    "FROM foreclosures WHERE archived_at IS NULL "
                    "ORDER BY case_number_raw, auction_date DESC"
                )
            ).fetchall()
            case_map: dict[str, int] = {r[1]: r[0] for r in rows}
            strap_map: dict[str, int] = {r[2]: r[0] for r in rows if r[2]}

            # Group extracted JSONs by case directory so we process each case
            # exactly once, choosing the best candidate.
            case_jsons: dict[str, list[Path]] = {}
            for json_path in FORECLOSURE_DATA_DIR.rglob("*_extracted.json"):
                # Only consider files inside a documents/ subdirectory
                if json_path.parent.name != "documents":
                    continue
                case_number = json_path.parent.parent.name
                case_jsons.setdefault(case_number, []).append(json_path)

            updated = 0
            for case_number, json_paths in case_jsons.items():
                best = self._select_best_judgment(json_paths)
                if best is None:
                    continue
                chosen_json_path, jd = best

                fid = case_map.get(case_number)
                if not fid:
                    parcel_id = jd.get("parcel_id", "")
                    if parcel_id:
                        fid = strap_map.get(parcel_id)
                if not fid:
                    continue

                # Derive the PDF path from the chosen JSON's stem:
                #   final_judgment_2026007828_extracted.json -> final_judgment_2026007828.pdf
                matching_pdf = chosen_json_path.parent / f"{chosen_json_path.stem.removesuffix('_extracted')}.pdf"
                pdf_path = str(matching_pdf) if matching_pdf.exists() else None

                if len(json_paths) > 1:
                    logger.info(
                        f"Case {case_number}: {len(json_paths)} extracted JSONs, "
                        f"chose {chosen_json_path.name} (pdf={pdf_path})"
                    )

                fja = jd.get("total_judgment_amount")

                conn.execute(
                    text(
                        "UPDATE foreclosures SET "
                        "  judgment_data = CAST(:jd AS jsonb), "
                        "  pdf_path = COALESCE(:pp, pdf_path), "
                        "  final_judgment_amount = COALESCE(:fja, final_judgment_amount), "
                        "  step_pdf_downloaded = COALESCE("
                        "      step_pdf_downloaded, "
                        "      CASE WHEN COALESCE(:pp, pdf_path, '') <> '' THEN now() END"
                        "  ), "
                        "  step_judgment_extracted = COALESCE(step_judgment_extracted, now()) "
                        "WHERE foreclosure_id = :fid"
                    ),
                    {
                        "jd": json.dumps(jd),
                        "pp": pdf_path,
                        "fja": fja,
                        "fid": fid,
                    },
                )
                updated += 1

        return updated
