"""Final judgment extraction pipeline for foreclosure PDFs.

This processor orchestrates multi-page vision extraction for Florida Final
Judgment of Foreclosure PDFs. It sits between ``VisionService`` and the PG
loading path in ``PgJudgmentService``:

1. Render PDF pages to images.
2. Ask the vision model for structured JSON using the canonical
   ``JudgmentExtraction`` schema.
3. Merge partial page/batch candidates into a single judgment candidate.
4. Validate the candidate against hard judgment quality gates.
5. Cache the full result, including validation metadata, so later services can
   distinguish a usable judgment from a captured-but-invalid attempt.

The core rule is that a model response is not considered successful just
because it parsed as JSON. A final judgment is only good if the extracted data
is internally coherent enough to drive downstream title, survival, and bidding
analysis.
"""

from __future__ import annotations

import json
import os
from contextlib import suppress
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import fitz  # PyMuPDF
from loguru import logger
from pydantic import ValidationError

from src.models.judgment_extraction import JudgmentExtraction
from src.services.vision_service import VisionService, robust_json_parse


class FinalJudgmentProcessor:
    """Process final-judgment PDFs into validated structured candidates."""

    _CACHE_FORMAT_VERSION = 2
    _BATCH_SIZE = 3
    _SCHEMA_NAME = "JudgmentExtraction"
    _CASE_FIELDS = (
        "case_number",
        "court_circuit",
        "county",
        "judge_name",
        "judgment_date",
        "foreclosure_sale_date",
        "sale_location",
        "is_online_sale",
        "plaintiff",
        "plaintiff_type",
        "foreclosure_type",
    )
    _PROPERTY_FIELDS = (
        "property_address",
        "legal_description",
        "parcel_id",
        "subdivision",
        "lot",
        "block",
        "unit",
        "plat_book",
        "plat_page",
        "is_condo",
    )
    _FINANCIAL_FIELDS = (
        "principal_amount",
        "interest_amount",
        "interest_through_date",
        "per_diem_rate",
        "per_diem_interest",
        "late_charges",
        "escrow_advances",
        "title_search_costs",
        "court_costs",
        "attorney_fees",
        "other_costs",
        "total_judgment_amount",
        "plaintiff_maximum_bid",
        "monthly_payment",
        "default_date",
    )
    _NESTED_FIELDS = ("foreclosed_mortgage", "lis_pendens")

    def __init__(self) -> None:
        self.vision_service = VisionService()
        self.temp_dir = Path("data/temp/doc_images")
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self._response_format = {
            "type": "json_schema",
            "json_schema": {
                "name": "final_judgment_extraction",
                "schema": JudgmentExtraction.model_json_schema(),
                "strict": True,
            },
        }
        self._schema_json = json.dumps(
            JudgmentExtraction.model_json_schema(),
            ensure_ascii=True,
        )

    @staticmethod
    def _json_cache_path(pdf_path: str) -> Path:
        """Return the path to the JSON extraction cache file next to the PDF."""
        p = Path(pdf_path)
        return p.parent / f"{p.stem}_extracted.json"

    @classmethod
    def _cache_is_current(cls, cached: dict[str, Any]) -> bool:
        metadata = cached.get("_metadata") or {}
        validation = cached.get("_validation") or {}
        return (
            isinstance(metadata, dict)
            and metadata.get("cache_format_version") == cls._CACHE_FORMAT_VERSION
            and isinstance(validation, dict)
            and "is_valid" in validation
        )

    def process_pdf(
        self,
        pdf_path: str,
        case_number: str,
        *,
        force: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        Process a Final Judgment PDF and extract structured data.

        Args:
            pdf_path: Path to the Final Judgment document (PDF, etc.)
            case_number: Case number for logging/tracking
            force: If True, ignore cached JSON and re-extract via Vision

        Returns:
            Dict with extracted data (including raw_text for debugging) or None if processing failed
        """
        if not os.path.exists(pdf_path):
            logger.error(f"PDF not found: {pdf_path}")
            return None

        # Check for cached extraction JSON next to the PDF
        cache_path = self._json_cache_path(pdf_path)
        if not force and cache_path.exists():
            try:
                cached = json.loads(cache_path.read_text(encoding="utf-8"))
                if cached and isinstance(cached, dict) and self._cache_is_current(cached):
                    logger.info(f"Loaded cached extraction for {case_number} from {cache_path.name}")
                    return cached
                logger.info(
                    "Cached extraction for {} is stale or missing validation metadata; re-extracting",
                    case_number,
                )
            except Exception as e:
                logger.warning(f"Bad cache file {cache_path}, re-extracting: {e}")

        page_images: list[str] = []
        try:
            logger.info(f"Processing Final Judgment PDF for case {case_number}...")

            # Open PDF with PyMuPDF
            doc = fitz.open(pdf_path)
            total_pages = len(doc)
            num_pages = total_pages  # Process all pages; chunked extraction avoids context issues

            logger.info(f"PDF has {total_pages} pages, rendering all pages")

            # Render all pages to images
            for page_num in range(num_pages):
                page = doc[page_num]
                temp_image_path = self.temp_dir / f"{case_number}_page_{page_num + 1}.png"
                pix = page.get_pixmap(dpi=150)
                pix.save(str(temp_image_path))
                page_images.append(str(temp_image_path))

            doc.close()

            merged_json: Optional[dict[str, Any]] = None
            strategies: list[str] = []

            # First pass: prioritize first 3 pages + last 5 pages (often contains Exhibit A)
            priority_images = self._select_priority_pages(page_images)
            if priority_images:
                strategies.append("priority_pages")
                logger.info(
                    f"Extracting from {len(priority_images)} prioritized pages..."
                )
                merged_json = self._extract_in_batches(priority_images, batch_size=self._BATCH_SIZE)

            # Second pass: chunk the entire document if critical fields are missing
            if self._needs_full_pass(merged_json):
                strategies.append("chunked_full")
                logger.info(
                    f"Running chunked extraction across {len(page_images)} pages..."
                )
                full_result = self._extract_in_batches(page_images, batch_size=self._BATCH_SIZE)
                if merged_json and full_result:
                    merged_json = self._merge_page_data([merged_json, full_result])
                else:
                    merged_json = merged_json or full_result

            # Final fallback: per-page extraction if still missing critical fields
            if self._needs_full_pass(merged_json):
                strategies.append("per_page_fallback")
                logger.info("Running per-page extraction fallback...")
                page_data_list: list[dict[str, Any]] = []
                if merged_json:
                    page_data_list.append(merged_json)
                for image_path in page_images:
                    page_result = self._extract_candidate_from_images([image_path])
                    if page_result:
                        page_data_list.append(page_result)
                merged_json = (
                    self._merge_page_data(page_data_list)
                    if page_data_list
                    else merged_json
                )

            if not merged_json:
                logger.warning(
                    "No structured data extracted for case {} ({} pages)",
                    case_number,
                    total_pages,
                )
                return None

            assessment = self.validation_summary(merged_json)
            if not assessment["is_valid"]:
                strategies.append("repair_pass")
                logger.warning(
                    "Final judgment extraction for case {} failed validation: {}",
                    case_number,
                    "; ".join(assessment["failures"]),
                )
                repaired_json = self._repair_candidate(
                    page_images,
                    merged_json,
                    assessment["failures"],
                )
                if repaired_json:
                    repaired_assessment = self.validation_summary(repaired_json)
                    if self._candidate_rank(repaired_json, repaired_assessment) > self._candidate_rank(
                        merged_json,
                        assessment,
                    ):
                        merged_json = repaired_json
                        assessment = repaired_assessment
                        logger.info(
                            "Repair pass improved judgment extraction for case {}: valid={} failures={}",
                            case_number,
                            assessment["is_valid"],
                            len(assessment["failures"]),
                        )

            # Save raw OCR text to disk for troubleshooting
            raw_text = merged_json.get("raw_text", "")
            if raw_text and case_number:
                try:
                    docs_dir = Path(f"data/Foreclosure/{case_number}/documents")
                    docs_dir.mkdir(parents=True, exist_ok=True)
                    ocr_path = docs_dir / f"{case_number}_raw_ocr.txt"
                    ocr_path.write_text(raw_text, encoding="utf-8")
                except Exception as exc:
                    # Non-fatal debug artifact: extraction should continue.
                    logger.debug(f"Could not write OCR debug text for {case_number}: {exc}")

            merged_json["_metadata"] = {
                "case_number": case_number,
                "pages_processed": num_pages,
                "total_pages": total_pages,
                "extraction_strategies": strategies,
                "cache_format_version": self._CACHE_FORMAT_VERSION,
                "schema_name": self._SCHEMA_NAME,
                "vision_model": self.vision_service.active_model,
            }
            merged_json["_validation"] = assessment

            # Save extraction to JSON cache next to the PDF
            try:
                cache_path.write_text(
                    json.dumps(merged_json, indent=2, default=str),
                    encoding="utf-8",
                )
                logger.info(f"Saved extraction cache: {cache_path.name}")
            except Exception as e:
                logger.warning(f"Failed to save extraction cache: {e}")

            logger.info(
                "Successfully processed Final Judgment for case {} (valid={})",
                case_number,
                assessment["is_valid"],
            )
            return merged_json

        except Exception as e:
            logger.error(f"Error processing PDF for case {case_number}: {e}")
            return None
        finally:
            # Clean up temp images
            for img_path in page_images:
                with suppress(Exception):
                    Path(img_path).unlink()
    
    def _build_extraction_prompt(
        self,
        *,
        current_candidate: dict[str, Any] | None = None,
        validation_failures: list[str] | None = None,
    ) -> str:
        prompt = f"""
You are extracting a Hillsborough County Florida Final Judgment of Foreclosure.
Respond with exactly one JSON object that conforms to the provided JSON Schema.

Critical rules:
- Include every declared key exactly once. Use null when unknown. Do not emit extra keys.
- Never output markdown fences or explanatory text.
- legal_description must be verbatim. If Exhibit A / Schedule A contains a fuller legal, use that version.
- property_address often appears in the sale paragraph or legal-description exhibit. Capture it if stated.
- judgment_date means the entered/filed date of the judgment order, not the hearing date, unless no filed/entered date appears.
- judge_name means the signing judge on the order, not a judge mentioned only in procedural history.
- attorney_fees is the awarded fee total, not the hourly rate.
- court_costs should include filing, service, publication, and online-sale fees when those are itemized as court costs.
- Use other_costs for residual named monetary items that do not fit the explicit fields.
- If the document states a total_judgment_amount, review the line items until they reconcile to that total within about one dollar. If a separate accrued per-diem carry amount is stated, put it in per_diem_interest.
- If the sale is at hillsborough.realforeclose.com or another web URL, set is_online_sale=true.

JSON Schema:
{self._schema_json}
""".strip()
        if current_candidate is not None and validation_failures:
            prompt += (
                "\n\nCurrent candidate to repair:\n"
                f"{json.dumps(self._strip_private_keys(current_candidate), ensure_ascii=True)}"
                "\n\nValidation failures that must be corrected if the document supports it:\n"
                f"{json.dumps(validation_failures, ensure_ascii=True)}"
                "\n\nRe-read the document images and return a corrected full JSON object. "
                "Do not preserve a prior value if the document contradicts it."
            )
        return prompt

    def _extract_candidate_from_images(
        self,
        image_paths: list[str],
        *,
        current_candidate: dict[str, Any] | None = None,
        validation_failures: list[str] | None = None,
    ) -> Optional[dict[str, Any]]:
        if not image_paths:
            return None
        prompt = self._build_extraction_prompt(
            current_candidate=current_candidate,
            validation_failures=validation_failures,
        )
        raw = self.vision_service.analyze_images(
            image_paths,
            prompt,
            max_tokens=6000,
            response_format=self._response_format,
        )
        if not raw:
            return None
        parsed = robust_json_parse(raw, "final_judgment_structured")
        if parsed is None:
            logger.warning("Judgment extraction returned non-JSON output")
        return parsed

    @staticmethod
    def _strip_private_keys(data: dict[str, Any]) -> dict[str, Any]:
        return {
            key: value
            for key, value in data.items()
            if not str(key).startswith("_")
        }

    @classmethod
    def _validation_messages(cls, exc: ValidationError) -> list[str]:
        messages: list[str] = []
        for err in exc.errors():
            msg = str(err.get("msg") or "validation error")
            if msg.startswith("Value error, "):
                msg = msg.removeprefix("Value error, ")
            messages.append(msg)
        return messages or [str(exc)]

    @classmethod
    def validation_summary(cls, candidate: Optional[dict[str, Any]]) -> dict[str, Any]:
        if not candidate:
            return {
                "is_valid": False,
                "failures": ["No structured judgment data extracted"],
                "warnings": [],
            }
        try:
            model = JudgmentExtraction.model_validate(cls._strip_private_keys(candidate))
        except ValidationError as exc:
            return {
                "is_valid": False,
                "failures": cls._validation_messages(exc),
                "warnings": [],
            }

        failures, warnings = model.validate_extraction()
        return {
            "is_valid": not failures,
            "failures": failures,
            "warnings": warnings,
        }

    @classmethod
    def canonicalize_candidate(cls, candidate: dict[str, Any]) -> dict[str, Any]:
        model = JudgmentExtraction.model_validate(cls._strip_private_keys(candidate))
        return model.model_dump(mode="json")

    @staticmethod
    def _is_present(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, str):
            return bool(value.strip())
        if isinstance(value, list):
            return bool(value)
        if isinstance(value, dict):
            return any(FinalJudgmentProcessor._is_present(v) for v in value.values())
        if isinstance(value, bool):
            return value
        return True

    @classmethod
    def _completeness_score(cls, candidate: dict[str, Any]) -> int:
        score = 0
        for value in cls._strip_private_keys(candidate).values():
            if isinstance(value, str) and value.strip():
                score += min(len(value.strip()), 200)
            elif isinstance(value, list):
                score += len(value) * 10
            elif isinstance(value, dict):
                score += sum(5 for item in value.values() if cls._is_present(item))
            elif cls._is_present(value):
                score += 10
        return score

    @classmethod
    def _candidate_rank(
        cls,
        candidate: dict[str, Any],
        summary: dict[str, Any],
    ) -> tuple[int, int, int, int]:
        return (
            1 if summary.get("is_valid") else 0,
            -len(summary.get("failures") or []),
            cls._completeness_score(candidate),
            -len(summary.get("warnings") or []),
        )

    @classmethod
    def _financial_score(cls, candidate: dict[str, Any]) -> tuple[int, int, float]:
        present = sum(1 for field in cls._FINANCIAL_FIELDS if cls._is_present(candidate.get(field)))
        total = candidate.get("total_judgment_amount")
        if total is None:
            return (0, present, float("-inf"))
        components = [
            candidate.get("principal_amount"),
            candidate.get("interest_amount"),
            candidate.get("per_diem_interest"),
            candidate.get("late_charges"),
            candidate.get("escrow_advances"),
            candidate.get("title_search_costs"),
            candidate.get("court_costs"),
            candidate.get("attorney_fees"),
            candidate.get("other_costs"),
        ]
        non_null = [float(v) for v in components if isinstance(v, (int, float))]
        if not non_null:
            return (1, present, float("-inf"))
        diff = abs(sum(non_null) - float(total))
        return (1, present, -diff)

    @classmethod
    def _merge_defendants(cls, page_data_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        order: list[str] = []
        for page_data in page_data_list:
            for defendant in page_data.get("defendants") or []:
                if not isinstance(defendant, dict):
                    continue
                name = str(defendant.get("name") or "").strip()
                if not name:
                    continue
                key = name.upper()
                if key not in merged:
                    merged[key] = deepcopy(defendant)
                    order.append(key)
                    continue
                existing = merged[key]
                for field, value in defendant.items():
                    if field == "name":
                        continue
                    if not cls._is_present(existing.get(field)) and cls._is_present(value):
                        existing[field] = value
        return [merged[key] for key in order]

    @classmethod
    def _merge_string_lists(
        cls,
        page_data_list: list[dict[str, Any]],
        field: str,
    ) -> list[str]:
        seen: set[str] = set()
        merged: list[str] = []
        for page_data in page_data_list:
            for value in page_data.get(field) or []:
                text = str(value).strip()
                if not text:
                    continue
                key = text.upper()
                if key in seen:
                    continue
                seen.add(key)
                merged.append(text)
        return merged

    @classmethod
    def _merge_red_flags(cls, page_data_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[tuple[str, str, str]] = set()
        merged: list[dict[str, Any]] = []
        for page_data in page_data_list:
            for flag in page_data.get("red_flags") or []:
                if not isinstance(flag, dict):
                    continue
                key = (
                    str(flag.get("flag_type") or "").strip(),
                    str(flag.get("severity") or "").strip(),
                    str(flag.get("description") or "").strip(),
                )
                if not any(key):
                    continue
                if key in seen:
                    continue
                seen.add(key)
                merged.append(deepcopy(flag))
        return merged

    @classmethod
    def _best_candidate_for_fields(
        cls,
        page_data_list: list[dict[str, Any]],
        fields: tuple[str, ...],
        *,
        scorer: Callable[[dict[str, Any]], Any] | None = None,
    ) -> dict[str, Any]:
        if not page_data_list:
            return {}
        if scorer is None:
            def scorer(candidate: dict[str, Any]) -> int:
                return sum(1 for field in fields if cls._is_present(candidate.get(field)))
        return max(page_data_list, key=scorer)

    def _merge_page_data(self, page_data_list: list[dict[str, Any]]) -> dict[str, Any]:
        """Merge multiple page/batch candidates with section-aware scoring."""
        candidates = [
            self._strip_private_keys(candidate)
            for candidate in page_data_list
            if isinstance(candidate, dict)
        ]
        if not candidates:
            return {}

        merged = deepcopy(max(candidates, key=self._completeness_score))

        best_case = self._best_candidate_for_fields(
            candidates,
            self._CASE_FIELDS,
        )
        best_property = self._best_candidate_for_fields(
            candidates,
            self._PROPERTY_FIELDS,
            scorer=lambda candidate: (
                len(str(candidate.get("legal_description") or "")),
                sum(1 for field in self._PROPERTY_FIELDS if self._is_present(candidate.get(field))),
            ),
        )
        best_financial = self._best_candidate_for_fields(
            candidates,
            self._FINANCIAL_FIELDS,
            scorer=self._financial_score,
        )

        for field in self._CASE_FIELDS:
            if self._is_present(best_case.get(field)):
                merged[field] = deepcopy(best_case[field])
        for field in self._PROPERTY_FIELDS:
            if self._is_present(best_property.get(field)):
                merged[field] = deepcopy(best_property[field])
        for field in self._FINANCIAL_FIELDS:
            if self._is_present(best_financial.get(field)):
                merged[field] = deepcopy(best_financial[field])

        merged["defendants"] = self._merge_defendants(candidates)
        merged["red_flags"] = self._merge_red_flags(candidates)
        merged["unclear_sections"] = self._merge_string_lists(candidates, "unclear_sections")

        for nested_field in self._NESTED_FIELDS:
            nested_candidates = [
                candidate.get(nested_field)
                for candidate in candidates
                if isinstance(candidate.get(nested_field), dict)
            ]
            if not nested_candidates:
                continue
            merged[nested_field] = deepcopy(
                max(
                    nested_candidates,
                    key=lambda value: sum(1 for item in value.values() if self._is_present(item)),
                )
            )

        for candidate in candidates:
            for key, value in candidate.items():
                if key in {"defendants", "red_flags", "unclear_sections", *self._NESTED_FIELDS}:
                    continue
                if not self._is_present(merged.get(key)) and self._is_present(value):
                    merged[key] = deepcopy(value)

        return merged

    def _extract_in_batches(
        self,
        image_paths: list[str],
        batch_size: int = 3,
    ) -> Optional[dict[str, Any]]:
        """Extract final judgment data in smaller batches to reduce timeouts."""
        if not image_paths:
            return None
        page_data_list: list[dict[str, Any]] = []
        for i in range(0, len(image_paths), batch_size):
            batch = image_paths[i:i + batch_size]
            batch_result = self._extract_candidate_from_images(batch)
            if batch_result:
                page_data_list.append(batch_result)
        return self._merge_page_data(page_data_list) if page_data_list else None

    def _repair_candidate(
        self,
        image_paths: list[str],
        current_candidate: dict[str, Any],
        validation_failures: list[str],
    ) -> Optional[dict[str, Any]]:
        return self._extract_candidate_from_images(
            image_paths,
            current_candidate=current_candidate,
            validation_failures=validation_failures,
        )

    def _select_priority_pages(self, page_images: list[str]) -> list[str]:
        """
        Select priority pages: first 3 pages + last 5 pages (often contains Exhibit A).
        Deduplicates if the document is short.
        """
        if not page_images:
            return []
        total = len(page_images)
        head_count = min(3, total)
        tail_count = min(5, max(0, total - head_count))
        selected = page_images[:head_count]
        if tail_count:
            selected += page_images[-tail_count:]
        # Deduplicate while preserving order
        seen = set()
        deduped = []
        for path in selected:
            if path in seen:
                continue
            seen.add(path)
            deduped.append(path)
        return deduped

    def _needs_full_pass(self, extracted_data: Optional[Dict[str, Any]]) -> bool:
        """Determine if we need a deeper pass to capture critical fields."""
        if not extracted_data:
            return True
        defendants = extracted_data.get('defendants') or []
        has_defendants = any(d.get('name') for d in defendants)
        legal_description = (extracted_data.get('legal_description') or "").strip()
        mortgage = extracted_data.get('foreclosed_mortgage') or {}
        has_mortgage_ref = any(
            mortgage.get(key)
            for key in ('instrument_number', 'recording_book', 'recording_page')
        )
        has_amount = bool(
            extracted_data.get('total_judgment_amount')
            or extracted_data.get('principal_amount')
        )
        return not (has_defendants and legal_description and (has_mortgage_ref or has_amount))
    
    @staticmethod
    def is_thin_extraction(result: Optional[Dict[str, Any]]) -> bool:
        """
        Check if extraction result is missing critical foreclosure fields.

        A "thin" extraction means the PDF probably isn't the real Final Judgment
        (e.g. a fee order from a CC case).  Recovery should be attempted.
        """
        if not result:
            return True
        legal_desc = (result.get("legal_description") or "").strip()
        mortgage = result.get("foreclosed_mortgage") or {}
        has_mortgage_ref = any(
            mortgage.get(k)
            for k in ("instrument_number", "recording_book", "recording_page")
        )
        return not legal_desc and not has_mortgage_ref

    @staticmethod
    def dump_pdf_text(pdf_path: str, case_number: str) -> Optional[str]:
        """
        Extract full text from PDF via PyMuPDF and dump to a debug file.

        Returns the path to the dump file, or None on failure.
        """
        try:
            dump_dir = Path("data/Foreclosure") / case_number / "debug"
            dump_dir.mkdir(parents=True, exist_ok=True)
            dump_path = dump_dir / "pdf_full_text.txt"

            doc = fitz.open(pdf_path)
            lines = []
            for page_num in range(len(doc)):
                page = doc[page_num]
                text = page.get_text("text")
                lines.append(f"--- PAGE {page_num + 1} ---")
                lines.append(text)
            doc.close()

            full_text = "\n".join(lines)
            dump_path.write_text(full_text, encoding="utf-8")
            logger.info(f"Dumped PDF text ({len(full_text)} chars) to {dump_path}")
            return str(dump_path)
        except Exception as e:
            logger.warning(f"Failed to dump PDF text for {case_number}: {e}")
            return None

    def _clean_amount(self, amount_str: Optional[str]) -> Optional[float]:
        """
        Clean and parse dollar amount strings.
        
        Args:
            amount_str: String like "$123,456.78" or "123456.78", or a number
            
        Returns:
            Float value or None if parsing fails
        """
        if not amount_str:
            return None
        
        # If already a number, return it
        if isinstance(amount_str, (int, float)):
            return float(amount_str) if amount_str != 0 else None
        
        try:
            # Remove $, commas, and whitespace
            cleaned = str(amount_str).replace('$', '').replace(',', '').strip()
            value = float(cleaned)
            return value if value != 0 else None
        except (ValueError, AttributeError):
            return None
    
    def extract_key_amounts(self, extracted_data: Dict[str, Any]) -> Dict[str, Optional[float]]:
        """
        Extract and clean key dollar amounts from the extracted data.

        Args:
            extracted_data: Raw data dict from vision service

        Returns:
            Dict with cleaned float amounts
        """
        # Handle nested foreclosed_mortgage structure
        mortgage_data = extracted_data.get('foreclosed_mortgage', {}) or {}

        return {
            'total_judgment_amount': self._clean_amount(extracted_data.get('total_judgment_amount')),
            'principal_amount': self._clean_amount(extracted_data.get('principal_amount')),
            'interest_amount': self._clean_amount(extracted_data.get('interest_amount')),
            'attorney_fees': self._clean_amount(extracted_data.get('attorney_fees')),
            'court_costs': self._clean_amount(extracted_data.get('court_costs')),
            'original_mortgage_amount': self._clean_amount(mortgage_data.get('original_amount')),
            'monthly_payment': self._clean_amount(extracted_data.get('monthly_payment')),
            'escrow_advances': self._clean_amount(extracted_data.get('escrow_advances')),
            'late_charges': self._clean_amount(extracted_data.get('late_charges')),
            'per_diem_rate': self._clean_amount(extracted_data.get('per_diem_rate')),
        }

    def extract_dates(self, extracted_data: Dict[str, Any]) -> Dict[str, Optional[str]]:
        """
        Extract key dates from the extracted data.

        Args:
            extracted_data: Raw data dict from vision service

        Returns:
            Dict with date strings in YYYY-MM-DD format
        """
        mortgage_data = extracted_data.get('foreclosed_mortgage', {}) or {}
        lis_pendens_data = extracted_data.get('lis_pendens', {}) or {}

        return {
            'judgment_date': extracted_data.get('judgment_date'),
            'foreclosure_sale_date': extracted_data.get('foreclosure_sale_date'),
            'default_date': extracted_data.get('default_date'),
            'original_mortgage_date': mortgage_data.get('original_date'),
            'lis_pendens_date': lis_pendens_data.get('recording_date'),
            'interest_through_date': extracted_data.get('interest_through_date'),
        }

    def extract_parties(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract party information from the extracted data.

        Args:
            extracted_data: Raw data dict from vision service

        Returns:
            Dict with plaintiff, defendant(s), and party analysis
        """
        defendants = extracted_data.get('defendants', []) or []

        # Flatten defendants list to string for storage
        defendant_names = [d.get('name', '') for d in defendants if d.get('name')]

        # Check for federal entities
        has_federal = any(d.get('is_federal_entity', False) for d in defendants)
        federal_defendants = [d.get('name') for d in defendants if d.get('is_federal_entity')]

        # Check for deceased borrowers
        has_deceased = any(d.get('is_deceased', False) for d in defendants)

        return {
            'plaintiff': extracted_data.get('plaintiff'),
            'plaintiff_type': extracted_data.get('plaintiff_type'),
            'defendant': ', '.join(defendant_names),
            'defendants_list': defendants,
            'has_federal_defendant': has_federal,
            'federal_defendants': federal_defendants,
            'has_deceased_borrower': has_deceased,
        }

    def extract_property_info(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract property information from the extracted data.

        Args:
            extracted_data: Raw data dict from vision service

        Returns:
            Dict with property details including legal description
        """
        return {
            'property_address': extracted_data.get('property_address'),
            'legal_description': extracted_data.get('legal_description'),
            'parcel_id': extracted_data.get('parcel_id'),
            'subdivision': extracted_data.get('subdivision'),
            'lot': extracted_data.get('lot'),
            'block': extracted_data.get('block'),
            'unit': extracted_data.get('unit'),
            'plat_book': extracted_data.get('plat_book'),
            'plat_page': extracted_data.get('plat_page'),
            'is_condo': extracted_data.get('is_condo', False),
        }

    def extract_recording_refs(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract recording references from the extracted data.

        Args:
            extracted_data: Raw data dict from vision service

        Returns:
            Dict with book/page and instrument references
        """
        mortgage_data = extracted_data.get('foreclosed_mortgage', {}) or {}
        lis_pendens_data = extracted_data.get('lis_pendens', {}) or {}

        return {
            'mortgage_book': mortgage_data.get('recording_book'),
            'mortgage_page': mortgage_data.get('recording_page'),
            'mortgage_instrument': mortgage_data.get('instrument_number'),
            'lis_pendens_book': lis_pendens_data.get('recording_book'),
            'lis_pendens_page': lis_pendens_data.get('recording_page'),
            'lis_pendens_instrument': lis_pendens_data.get('instrument_number'),
        }

    def extract_red_flags(self, extracted_data: Dict[str, Any]) -> list:
        """
        Extract red flags from the extracted data.

        Args:
            extracted_data: Raw data dict from vision service

        Returns:
            List of red flag dictionaries
        """
        return extracted_data.get('red_flags', []) or []

    def get_foreclosure_type(self, extracted_data: Dict[str, Any]) -> Optional[str]:
        """
        Get the foreclosure type from the extracted data.

        Args:
            extracted_data: Raw data dict from vision service

        Returns:
            Foreclosure type string
        """
        fc_type = extracted_data.get('foreclosure_type')
        if fc_type:
            # Normalize to our expected values
            fc_type = fc_type.upper().strip()
            if 'FIRST' in fc_type or 'PRIMARY' in fc_type:
                return 'FIRST MORTGAGE'
            if 'SECOND' in fc_type or 'JUNIOR' in fc_type or 'HELOC' in fc_type:
                return 'SECOND MORTGAGE'
            if 'HOA' in fc_type or 'CONDO' in fc_type or 'ASSOCIATION' in fc_type:
                return 'HOA'
            if 'TAX' in fc_type:
                return 'TAX'
            return fc_type
        return None


if __name__ == "__main__":
    # Test the processor
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python -m src.services.final_judgment_processor <pdf_path>")
        sys.exit(1)
    
    pdf_path = sys.argv[1]
    case_number = Path(pdf_path).stem.replace('_final_judgment', '')
    
    processor = FinalJudgmentProcessor()
    result = processor.process_pdf(pdf_path, case_number)
    
    if result:
        print("\n=== Extracted Data ===")
        import json
        print(json.dumps(result, indent=2))
        
        print("\n=== Cleaned Amounts ===")
        amounts = processor.extract_key_amounts(result)
        print(json.dumps(amounts, indent=2))
    else:
        print("Failed to extract data from PDF")
