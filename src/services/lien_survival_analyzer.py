"""
Lien Survival Analyzer that uses extracted Final Judgment metadata.

Relies on:
- foreclosure_type extracted from Final Judgment (no more guessing)
- original_mortgage_amount for HOA Safe Harbor calculations
- lis_pendens_date as cutoff for junior liens
"""
from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from src.models.property import Lien


class LienSurvivalAnalyzer:
    """Determine which liens survive a foreclosure based on judgment data."""

    SUPERPRIORITY_TYPES = ("TAX", "IRS", "MUNICIPAL", "UTILITY", "CODE")

    def __init__(self, monthly_hoa_dues: Optional[float] = None, months_unpaid: int = 12):
        self.monthly_hoa_dues = monthly_hoa_dues
        self.months_unpaid = months_unpaid

    def _is_superpriority(self, lien: Lien) -> bool:
        doc_type = (lien.document_type or "").upper()
        return any(tag in doc_type for tag in self.SUPERPRIORITY_TYPES)

    def _calculate_hoa_safe_harbor(self, original_mortgage_amount: float) -> Optional[float]:
        if original_mortgage_amount is None or self.monthly_hoa_dues is None:
            return None
        option_1 = min(self.months_unpaid, 12) * self.monthly_hoa_dues
        option_2 = original_mortgage_amount * 0.01
        return min(option_1, option_2)

    def _is_expired(self, lien: Lien) -> Tuple[bool, Optional[str]]:
        """
        Check if a lien has expired based on Florida statutes.
        Returns (is_expired, reason).
        """
        if not lien.recording_date:
            return False, None

        age_years = (datetime.now(tz=UTC).date() - lien.recording_date).days / 365.25
        doc_type = (lien.document_type or "").upper()

        # Mechanic's Liens (Construction Liens) - 1 year
        # Fla. Stat. 713.22
        if ("MECHANIC" in doc_type or "CONSTRUCTION" in doc_type) and age_years > 1:
            return True, "Expired Mechanic's Lien (>1 year)"

        # HOA/COA Claim of Lien - 1 year to file suit
        # Fla. Stat. 720.3085(1)(b) / 718.116(5)(b)
        # Note: If converted to Judgment, it lasts longer. We assume "Claim of Lien" here.
        if ("HOA" in doc_type or "CONDO" in doc_type or "ASSOCIATION" in doc_type) and "CLAIM" in doc_type and age_years > 1:
            return True, "Expired HOA Claim of Lien (>1 year without suit)"

        # Judgment Liens - 10 years (renewable to 20)
        # Fla. Stat. 55.10
        if "JUDGMENT" in doc_type and "FINAL" not in doc_type and age_years > 10:  # Exclude the current Final Judgment
            # We flag it as potentially expired, though it could have been re-recorded.
            # For safety, we might want to keep it if it's close, but >20 is definitely gone.
            if age_years > 20:
                return True, "Expired Judgment Lien (>20 years)"
            return True, "Likely Expired Judgment Lien (>10 years)"

        # Code Enforcement - 20 years
        # Fla. Stat. 162.09(3)
        if ("CODE" in doc_type or "ENFORCEMENT" in doc_type) and age_years > 20:
            return True, "Expired Code Enforcement Lien (>20 years)"

        return False, None

    def analyze(
        self,
        liens: List[Lien],
        foreclosure_type: Optional[str],
        lis_pendens_date: Optional[date],
        original_mortgage_amount: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Return surviving/wiped liens and any safe harbor amounts."""
        surviving: List[Lien] = []
        wiped: List[Lien] = []
        expired: List[Lien] = []

        for lien in liens:
            try:
                # Check for expiration first
                is_expired, reason = self._is_expired(lien)
                if is_expired:
                    lien.is_surviving = False
                    lien.notes = reason
                    expired.append(lien)
                    continue

                # Superpriority liens always survive
                if self._is_superpriority(lien):
                    lien.is_surviving = True
                    surviving.append(lien)
                    continue

                # If no Lis Pendens date, default to survive (conservative)
                if not lis_pendens_date or not lien.recording_date:
                    lien.is_surviving = True
                    surviving.append(lien)
                    continue

                # Date-based cutoff using lis pendens
                if lien.recording_date < lis_pendens_date:
                    lien.is_surviving = True
                    surviving.append(lien)
                else:
                    lien.is_surviving = False
                    wiped.append(lien)
            except Exception as exc:
                logger.error("Failed lien survival evaluation: {err}", err=exc)

        hoa_safe_harbor = None
        if foreclosure_type and foreclosure_type.upper() == "FIRST MORTGAGE":
            hoa_safe_harbor = self._calculate_hoa_safe_harbor(original_mortgage_amount or 0)

        return {
            "foreclosure_type": foreclosure_type,
            "lis_pendens_date": lis_pendens_date.isoformat() if lis_pendens_date else None,
            "original_mortgage_amount": original_mortgage_amount,
            "surviving_liens": surviving,
            "wiped_liens": wiped,
            "expired_liens": expired,
            "hoa_safe_harbor": hoa_safe_harbor,
        }
