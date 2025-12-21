"""
Party 2 Resolution Service

Resolves missing Party 2 (grantee) data for documents where ORI only indexed Party 1.

Resolution Strategies (in order):
1. CQID 326 Party Name Search - Search by Party 1's name to find Party 2
2. vLLM OCR Extraction - Download PDF and extract parties using vision AI

Also handles self-transfer detection when grantor and grantee are the same person.

See /docs/2ndparty.md for detailed documentation.
"""
import re
import tempfile
from contextlib import suppress
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from difflib import SequenceMatcher

from loguru import logger

from src.scrapers.ori_api_scraper import ORIApiScraper
from src.services.vision_service import VisionService


@dataclass
class Party2Resolution:
    """Result of Party 2 resolution attempt."""
    party2: Optional[str]
    method: str  # "cqid_326", "ocr_extraction", "unresolved"
    is_self_transfer: bool = False
    self_transfer_type: Optional[str] = None  # "exact_match", "trust_transfer", "name_variation"
    confidence: float = 1.0
    ocr_data: Optional[Dict[str, Any]] = None  # Full OCR extraction if used


class Party2ResolutionService:
    """
    Service to resolve missing Party 2 (grantee) data for ORI documents.

    The ORI (Official Records Index) often has incomplete party indexing,
    especially for deeds where Party 2 may be indexed under a different
    legal description text than Party 1.
    """

    # Deed document types that should have both parties
    DEED_TYPES = {
        "(D) DEED", "(WD) WARRANTY DEED", "(QC) QUIT CLAIM",
        "(CD) CORRECTIVE DEED", "(TD) TRUSTEE DEED", "(SD) SPECIAL WARRANTY DEED",
        "(TAXDEED) TAX DEED", "D", "WD", "QC", "CD", "TD", "SD", "TAXDEED"
    }

    def __init__(self, ori_scraper: Optional[ORIApiScraper] = None,
                 vision_service: Optional[VisionService] = None):
        """
        Initialize the service.

        Args:
            ori_scraper: ORI scraper instance (creates new if not provided)
            vision_service: Vision service instance (creates new if not provided)
        """
        self.ori_scraper = ori_scraper or ORIApiScraper()
        self.vision_service = vision_service or VisionService()

    def needs_resolution(self, doc: Dict[str, Any]) -> bool:
        """
        Check if a document needs Party 2 resolution.

        Args:
            doc: Document dictionary with doc_type, party1, party2 fields

        Returns:
            True if document is a deed type and missing Party 2
        """
        doc_type = doc.get("doc_type", "") or doc.get("document_type", "")
        party1 = doc.get("party1") or doc.get("grantor")
        party2 = doc.get("party2") or doc.get("grantee")

        # Normalize doc_type for comparison
        doc_type_upper = doc_type.upper()
        is_deed = any(dt.upper() in doc_type_upper for dt in self.DEED_TYPES)

        return bool(is_deed and party1 and not party2)

    def resolve_party2(self, doc: Dict[str, Any], output_dir: Optional[Path] = None) -> Party2Resolution:
        """
        Attempt to resolve missing Party 2 for a document (sync version).

        Args:
            doc: Document dictionary with instrument, party1, doc_type fields
            output_dir: Directory for PDF downloads (uses temp dir if not provided)

        Returns:
            Party2Resolution with results and method used
        """
        instrument = doc.get("instrument") or doc.get("instrument_number")
        party1 = doc.get("party1") or doc.get("grantor")

        if not instrument or not party1:
            return Party2Resolution(party2=None, method="unresolved")

        logger.info(f"Resolving Party 2 for instrument {instrument} (Party 1: {party1})")

        # Strategy 1: Try CQID 326 party name search
        party2 = self._try_cqid_326_search(party1, instrument)
        if party2:
            is_self_transfer, transfer_type = self._detect_self_transfer(party1, party2)
            return Party2Resolution(
                party2=party2,
                method="cqid_326",
                is_self_transfer=is_self_transfer,
                self_transfer_type=transfer_type
            )

        # Strategy 2: Download PDF and OCR
        result = self._try_ocr_extraction(doc, output_dir)
        if result:
            return result

        return Party2Resolution(party2=None, method="unresolved")

    async def resolve_party2_async(self, doc: Dict[str, Any], output_dir: Optional[Path] = None) -> Party2Resolution:
        """
        Attempt to resolve missing Party 2 for a document (async version).

        Use this when calling from an async context to avoid event loop conflicts.

        Args:
            doc: Document dictionary with instrument, party1, doc_type fields
            output_dir: Directory for PDF downloads (uses temp dir if not provided)

        Returns:
            Party2Resolution with results and method used
        """
        instrument = doc.get("instrument") or doc.get("instrument_number")
        party1 = doc.get("party1") or doc.get("grantor")

        if not instrument or not party1:
            return Party2Resolution(party2=None, method="unresolved")

        logger.info(f"Resolving Party 2 for instrument {instrument} (Party 1: {party1})")

        # Strategy 1: Try CQID 326 party name search (async)
        party2 = await self._try_cqid_326_search_async(party1, instrument)
        if party2:
            is_self_transfer, transfer_type = self._detect_self_transfer(party1, party2)
            return Party2Resolution(
                party2=party2,
                method="cqid_326",
                is_self_transfer=is_self_transfer,
                self_transfer_type=transfer_type
            )

        # Strategy 2: Download PDF and OCR (sync - vision service is HTTP-based, not browser)
        result = self._try_ocr_extraction(doc, output_dir)
        if result:
            return result

        return Party2Resolution(party2=None, method="unresolved")

    def _try_cqid_326_search(self, party1: str, instrument: str) -> Optional[str]:
        """
        Try to find Party 2 using CQID 326 party name search (sync version).

        Args:
            party1: Party 1 (grantor) name
            instrument: Target instrument number

        Returns:
            Party 2 name if found, None otherwise
        """
        try:
            party2 = self.ori_scraper.find_party2_for_instrument(party1, instrument)
            if party2:
                logger.info(f"Found Party 2 via CQID 326: {party2}")
            return party2
        except Exception as e:
            logger.warning(f"CQID 326 search failed: {e}")
            return None

    async def _try_cqid_326_search_async(self, party1: str, instrument: str) -> Optional[str]:
        """
        Try to find Party 2 using CQID 326 party name search (async version).

        Uses the async browser methods directly to avoid event loop conflicts.

        Args:
            party1: Party 1 (grantor) name
            instrument: Target instrument number

        Returns:
            Party 2 name if found, None otherwise
        """
        try:
            # Use async method directly
            party2 = await self.ori_scraper.find_party2_for_instrument_async(party1, instrument)
            if party2:
                logger.info(f"Found Party 2 via CQID 326: {party2}")
            return party2
        except Exception as e:
            logger.warning(f"CQID 326 search failed: {e}")
            return None

    def _try_ocr_extraction(self, doc: Dict[str, Any],
                           output_dir: Optional[Path] = None) -> Optional[Party2Resolution]:
        """
        Download PDF and extract parties using vLLM vision.

        Args:
            doc: Document dictionary
            output_dir: Directory for PDF downloads

        Returns:
            Party2Resolution if successful, None otherwise
        """
        # Check if vision server is available
        if not self.vision_service.check_server():
            logger.warning("Vision server not available for OCR extraction")
            return None

        # Use temp directory if not provided
        if output_dir is None:
            output_dir = Path(tempfile.mkdtemp())

        try:
            # If doc doesn't have ID, we need to search ORI API to get it
            ori_doc = doc
            if not doc.get("ID"):
                instrument = doc.get("instrument") or doc.get("instrument_number")
                legal = doc.get("legal") or doc.get("legal_description")
                party1 = doc.get("party1") or doc.get("grantor")

                if instrument:
                    logger.info(f"Searching ORI API to get document ID for instrument {instrument}")
                    instrument_str = str(instrument)

                    # Strategy 1: Search by legal description if we have it
                    if legal:
                        legal_search = " ".join(legal.split()[:4])
                        api_results = self.ori_scraper.search_by_legal(legal_search)
                        for result in api_results:
                            if str(result.get("Instrument", "")) == instrument_str:
                                ori_doc = result
                                logger.info(f"Found document ID via legal search: {result.get('ID')}")
                                break

                    # Strategy 2: Search by party name
                    if not ori_doc.get("ID") and party1:
                        # Use API party search (limited results but may contain our doc)
                        api_results = self.ori_scraper.search_by_party(party1)
                        for result in api_results:
                            if str(result.get("Instrument", "")) == instrument_str:
                                ori_doc = result
                                logger.info(f"Found document ID via party search: {result.get('ID')}")
                                break

                    if not ori_doc.get("ID"):
                        logger.warning(f"Could not find document ID for instrument {instrument} in ORI API")
                        return None

            # Download PDF
            pdf_path = self.ori_scraper.download_pdf(ori_doc, output_dir)
            if not pdf_path:
                logger.warning(f"Failed to download PDF for document {ori_doc.get('ID') or doc.get('instrument')}")
                return None

            # Convert PDF to images (multi-page support)
            image_paths = self._convert_pdf_to_images(pdf_path)
            if not image_paths:
                logger.warning(f"Failed to convert PDF to images: {pdf_path}")
                return None

            # Extract with vision service (multi-image)
            doc_type = (doc.get("doc_type") or doc.get("document_type") or "DEED")
            ocr_result = self.vision_service.extract_document_by_type_multi(
                [str(p) for p in image_paths],
                doc_type
            )
            if not ocr_result:
                logger.warning(f"OCR extraction returned no results")
                return None

            # Cleanup temp images
            with suppress(Exception):
                for p in image_paths:
                    p.unlink()

            party2 = ocr_result.get("grantee") or ocr_result.get("party2")
            party1 = doc.get("party1") or doc.get("grantor") or ocr_result.get("grantor")

            if party2:
                logger.info(f"Extracted Party 2 via OCR: {party2}")
                is_self_transfer, transfer_type = self._detect_self_transfer(party1, party2)
                return Party2Resolution(
                    party2=party2,
                    method="ocr_extraction",
                    is_self_transfer=is_self_transfer,
                    self_transfer_type=transfer_type,
                    confidence=0.85,  # OCR has lower confidence
                    ocr_data=ocr_result
                )

            return None

        except Exception as e:
            logger.error(f"OCR extraction failed: {e}")
            return None

    def _convert_pdf_to_images(self, pdf_path: Path, max_pages: int = 5) -> Optional[list[Path]]:
        """
        Convert PDF pages to PNG images.

        Args:
            pdf_path: Path to PDF file

        Returns:
            List of PNG image paths or None if failed
        """
        try:
            import pypdfium2 as pdfium

            pdf = pdfium.PdfDocument(str(pdf_path))
            if len(pdf) == 0:
                return None

            image_paths: list[Path] = []
            pages_to_render = min(len(pdf), max_pages)
            for idx in range(pages_to_render):
                page = pdf[idx]
                bitmap = page.render(scale=200/72)
                pil_image = bitmap.to_pil()
                image_path = pdf_path.with_suffix(f".page{idx+1}.png")
                pil_image.save(str(image_path))
                image_paths.append(image_path)

            return image_paths

        except ImportError:
            logger.error("pypdfium2 not installed. Run: uv add pypdfium2")
            return None
        except Exception as e:
            logger.error(f"PDF to image conversion failed: {e}")
            return None

    def _detect_self_transfer(self, party1: str, party2: str) -> Tuple[bool, Optional[str]]:
        """
        Detect if a deed is a self-transfer (same grantor and grantee).

        Self-transfers occur when:
        - Individual transfers to their own trust
        - Name changes (marriage/divorce)
        - Vesting changes (adding/removing spouse)
        - Corrective deeds

        Args:
            party1: Grantor name
            party2: Grantee name

        Returns:
            Tuple of (is_self_transfer, transfer_type)
        """
        if not party1 or not party2:
            return False, None

        # Normalize names
        p1_norm = self._normalize_name(party1)
        p2_norm = self._normalize_name(party2)

        # Check for exact match
        if p1_norm == p2_norm:
            return True, "exact_match"

        # Check for trust transfer (same base name + "trustee")
        p2_lower = party2.lower()
        if "trustee" in p2_lower or "trust" in p2_lower:
            base_name = self._extract_base_name(party2)
            if self._names_match(p1_norm, base_name):
                return True, "trust_transfer"

        # Check for fuzzy match (name variations)
        similarity = SequenceMatcher(None, p1_norm, p2_norm).ratio()
        if similarity > 0.85:
            return True, "name_variation"

        return False, None

    def _normalize_name(self, name: str) -> str:
        """
        Normalize a name for comparison.

        Removes:
        - Suffixes (JR, SR, II, III, IV)
        - Titles (MR, MRS, MS, DR)
        - Extra whitespace
        - Punctuation

        Then sorts parts alphabetically to handle "LAST FIRST" vs "FIRST LAST"
        """
        if not name:
            return ""

        name = name.upper()

        # Remove common suffixes
        suffixes = [" JR", " SR", " II", " III", " IV", " ESQ", " MD", " PHD"]
        for suffix in suffixes:
            name = name.removesuffix(suffix)

        # Remove titles
        titles = ["MR ", "MRS ", "MS ", "DR ", "MISS "]
        for title in titles:
            name = name.removeprefix(title)

        # Remove punctuation and extra whitespace
        name = re.sub(r"[.,;:]", "", name)
        name = re.sub(r"\s+", " ", name)
        name = name.strip()

        # Sort name parts alphabetically to handle "LAST FIRST" vs "FIRST LAST"
        # This makes "BARGAMIN KRISTEN H" and "KRISTEN H BARGAMIN" equivalent
        parts = sorted(name.split())
        return " ".join(parts)

    def _extract_base_name(self, trust_name: str) -> str:
        """
        Extract base person name from a trust name.

        Examples:
        - "JOHN SMITH, Trustee of the Smith Family Trust" -> "JOHN SMITH"
        - "THE SMITH FAMILY TRUST" -> "SMITH"
        """
        if not trust_name:
            return ""

        name = trust_name.upper()

        # Check for "NAME, Trustee of..." pattern
        if "TRUSTEE" in name:
            parts = re.split(r",?\s*(?:AS\s+)?TRUSTEE", name, maxsplit=1)
            if parts:
                return self._normalize_name(parts[0])

        # Check for "THE X FAMILY TRUST" pattern
        match = re.search(r"THE\s+(\w+)\s+FAMILY\s+TRUST", name)
        if match:
            return match.group(1)

        return self._normalize_name(name)

    def _names_match(self, name1: str, name2: str, threshold: float = 0.8) -> bool:
        """
        Check if two names match with fuzzy comparison.
        """
        if not name1 or not name2:
            return False

        n1 = self._normalize_name(name1)
        n2 = self._normalize_name(name2)

        if n1 == n2:
            return True

        # Check if one is contained in the other
        if n1 in n2 or n2 in n1:
            return True

        # Fuzzy match
        similarity = SequenceMatcher(None, n1, n2).ratio()
        return similarity >= threshold

    def resolve_batch(self, documents: List[Dict[str, Any]],
                     output_dir: Optional[Path] = None) -> Dict[str, Party2Resolution]:
        """
        Resolve Party 2 for a batch of documents.

        Args:
            documents: List of document dictionaries
            output_dir: Directory for PDF downloads

        Returns:
            Dictionary mapping instrument numbers to resolution results
        """
        results = {}

        for doc in documents:
            if not self.needs_resolution(doc):
                continue

            instrument = doc.get("instrument") or doc.get("instrument_number")
            if not instrument:
                continue

            result = self.resolve_party2(doc, output_dir)
            results[instrument] = result

            if result.party2:
                logger.info(f"Resolved {instrument}: {result.party2} ({result.method})"
                           f"{' [SELF-TRANSFER]' if result.is_self_transfer else ''}")
            else:
                logger.warning(f"Could not resolve Party 2 for {instrument}")

        return results


def detect_self_transfer(party1: str, party2: str) -> Tuple[bool, Optional[str]]:
    """
    Standalone function to detect self-transfers.

    Convenience function for use outside the service class.
    """
    service = Party2ResolutionService()
    return service._detect_self_transfer(party1, party2)  # noqa: SLF001
