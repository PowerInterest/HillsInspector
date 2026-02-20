"""Phase B Step 2: Extract judgment data from PDFs → PG foreclosures.judgment_data.

Finds foreclosures that have a PDF on disk but no extracted JSON, runs
VisionService extraction via FinalJudgmentProcessor, then pushes the JSON
cache into PG via the refresh path.
"""

from __future__ import annotations

import json
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

        logger.info(
            f"Found {len(needs_extract)} PDFs needing judgment extraction"
        )

        # Step 2: Process each PDF with FinalJudgmentProcessor
        extracted = self._extract_judgments(needs_extract)

        # Step 3: Push all JSON caches to PG (idempotent)
        loaded = self._load_judgment_data_to_pg()

        return {
            "pdfs_found": len(needs_extract),
            "pdfs_extracted": extracted,
            "judgments_loaded_to_pg": loaded,
        }

    def _find_unextracted_pdfs(
        self, limit: int | None
    ) -> list[dict[str, Any]]:
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

            # Check if JSON cache exists for any PDF
            has_json = any(
                (doc_dir / f"{pdf.stem}_extracted.json").exists()
                for pdf in pdfs
            )
            if has_json:
                continue

            results.append({
                "case_number": case_dir.name,
                "pdf_path": str(pdfs[0]),
            })

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
                        f"Extracted judgment for {case_number}: "
                        f"plaintiff={result.get('plaintiff', '?')}"
                    )
            except Exception as exc:
                logger.error(
                    f"Judgment extraction failed for {case_number}: {exc}"
                )

        return extracted

    def _load_judgment_data_to_pg(self) -> int:
        """Scan all _extracted.json files and push to PG foreclosures."""
        if not FORECLOSURE_DATA_DIR.exists():
            return 0

        with self.engine.begin() as conn:
            # Build lookup maps
            rows = conn.execute(
                text(
                    "SELECT foreclosure_id, case_number_raw, strap "
                    "FROM foreclosures"
                )
            ).fetchall()
            case_map: dict[str, int] = {r[1]: r[0] for r in rows}
            strap_map: dict[str, int] = {r[2]: r[0] for r in rows if r[2]}

            updated = 0
            for json_path in FORECLOSURE_DATA_DIR.rglob("*_extracted.json"):
                case_number = json_path.parent.parent.name

                try:
                    jd = json.loads(json_path.read_text())
                except (json.JSONDecodeError, OSError) as exc:
                    logger.warning(
                        f"Skipping invalid judgment JSON {json_path}: {exc}"
                    )
                    continue

                fid = case_map.get(case_number)
                if not fid:
                    parcel_id = jd.get("parcel_id", "")
                    if parcel_id:
                        fid = strap_map.get(parcel_id)
                if not fid:
                    continue

                pdf_path = None
                for p in json_path.parent.glob("*.pdf"):
                    pdf_path = str(p)
                    break

                fja = jd.get("total_judgment_amount")

                conn.execute(
                    text(
                        "UPDATE foreclosures SET "
                        "  judgment_data = CAST(:jd AS jsonb), "
                        "  pdf_path = COALESCE(:pp, pdf_path), "
                        "  final_judgment_amount = COALESCE(:fja, final_judgment_amount), "
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
