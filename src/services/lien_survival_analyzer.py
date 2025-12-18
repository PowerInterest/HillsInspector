"""
Lien Survival Analyzer that uses extracted Final Judgment metadata.

Determines which liens will survive the UPCOMING foreclosure sale based on:
- foreclosure_type: HOA, FIRST_MORTGAGE, SECOND_MORTGAGE, TAX_DEED, etc.
- current_owner_acquisition_date: Liens from prior owners are HISTORICAL
- lis_pendens_date: For priority determination
- Florida statutes for expiration and safe harbor rules

Survival Status Values:
- SURVIVED: Will survive the upcoming foreclosure sale (senior liens, superpriority)
- EXTINGUISHED: Will be wiped out by the upcoming foreclosure sale (junior liens)
- EXPIRED: Already expired by statute of limitations
- SATISFIED: Already paid off/released
- HISTORICAL: From a prior ownership period - already wiped by a previous foreclosure
- FORECLOSING: This is the lien being foreclosed (the plaintiff's lien)
"""
from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger



class LienSurvivalAnalyzer:
    """Determine which liens survive a foreclosure based on judgment data."""

    # Liens that ALWAYS survive any foreclosure (government priority)
    SUPERPRIORITY_TYPES = ("TAX", "IRS", "MUNICIPAL", "UTILITY", "CODE ENFORCEMENT", "PACE", "CLEAN ENERGY")

    # Institutional names that indicate foreclosure deed grantees
    FORECLOSURE_GRANTEE_KEYWORDS = (
        "BANK", "MORTGAGE", "FANNIE", "FREDDIE", "HUD", "HOUSING",
        "SECRETARY", "FEDERAL", "NATIONAL", "TRUST", "SERVICER",
        "WELLS FARGO", "CHASE", "CITI", "BOA", "AMERICA"
    )

    def __init__(self, monthly_hoa_dues: Optional[float] = None, months_unpaid: int = 12):
        self.monthly_hoa_dues = monthly_hoa_dues
        self.months_unpaid = months_unpaid

    def _is_superpriority(self, lien_type: str, creditor: str = "") -> bool:
        """Check if lien type is superpriority (survives all foreclosures)."""
        doc_type = (lien_type or "").upper()
        creditor_upper = (creditor or "").upper()
        
        # PACE Liens are superpriority
        if "PACE" in doc_type or "PACE" in creditor_upper or "CLEAN ENERGY" in doc_type:
            return True
            
        # Property Taxes and Municipal Utilities
        if "TAX" in doc_type and "DEED" not in doc_type: # Exclude Tax Deed itself
            return True
        
        # Municipal Utility Liens (Water/Sewer often superpriority)
        if "UTILITY" in doc_type or "WATER" in doc_type or "SEWER" in doc_type:
            return True
            
        return False

    def _is_federal_lien(self, lien_type: str, creditor: str) -> bool:
        """Check if lien is held by Federal Gov (IRS, DOJ, etc)."""
        doc_type = (lien_type or "").upper()
        creditor_upper = (creditor or "").upper()
        return "IRS" in doc_type or "INTERNAL REVENUE" in creditor_upper or "USA" in creditor_upper or "UNITED STATES" in creditor_upper

    def _is_first_mortgage(self, lien_type: str, creditor: str) -> bool:
        """Check if this appears to be a first mortgage."""
        doc_type = (lien_type or "").upper()
        creditor_upper = (creditor or "").upper()

        # Must be a mortgage type
        if "MORTGAGE" not in doc_type and "MTG" not in doc_type:
            return False

        # Check for institutional lender (likely first mortgage holder)
        return any(kw in creditor_upper for kw in self.FORECLOSURE_GRANTEE_KEYWORDS)

    def _calculate_hoa_safe_harbor(self, original_mortgage_amount: float) -> Optional[float]:
        """
        Calculate HOA Safe Harbor amount per Florida Statutes 720.3085 / 718.116.

        In an HOA/COA foreclosure, the first mortgage survives BUT the HOA can
        collect up to the lesser of:
        - 12 months of unpaid dues, OR
        - 1% of the original mortgage amount
        """
        if original_mortgage_amount is None or self.monthly_hoa_dues is None:
            return None
        option_1 = min(self.months_unpaid, 12) * self.monthly_hoa_dues
        option_2 = original_mortgage_amount * 0.01
        return min(option_1, option_2)

    def _is_expired(self, lien_type: str, recording_date: Optional[date]) -> Tuple[bool, Optional[str]]:
        """
        Check if a lien has expired based on Florida statutes.
        Returns (is_expired, reason).
        """
        if not recording_date:
            return False, None

        age_years = (datetime.now(tz=UTC).date() - recording_date).days / 365.25
        doc_type = (lien_type or "").upper()

        # Mechanic's Liens (Construction Liens) - 1 year to file suit
        # Fla. Stat. 713.22
        if ("MECHANIC" in doc_type or "CONSTRUCTION" in doc_type) and age_years > 1:
            return True, "Expired Mechanic's Lien (>1 year)"

        # HOA/COA Claim of Lien - 1 year to file suit
        # Fla. Stat. 720.3085(1)(b) / 718.116(5)(b)
        if ("HOA" in doc_type or "CONDO" in doc_type or "ASSOCIATION" in doc_type) and "CLAIM" in doc_type and age_years > 1:
            return True, "Expired HOA Claim of Lien (>1 year without suit)"

        # Judgment Liens - 10 years (renewable to 20)
        # Fla. Stat. 55.10
        if "JUDGMENT" in doc_type and age_years > 20:
            return True, "Expired Judgment Lien (>20 years)"
        if "JUDGMENT" in doc_type and age_years > 10:
            return True, "Likely Expired Judgment Lien (>10 years, not re-recorded)"

        # Code Enforcement - 20 years
        # Fla. Stat. 162.09(3)
        if ("CODE" in doc_type or "ENFORCEMENT" in doc_type) and age_years > 20:
            return True, "Expired Code Enforcement Lien (>20 years)"

        # Mortgages - 5 years after maturity (typically 30 years)
        # For safety, flag mortgages > 35 years old
        if ("MORTGAGE" in doc_type or "MTG" in doc_type) and age_years > 35:
            return True, "Likely Expired Mortgage (>35 years)"

        return False, None

    def _is_foreclosing_party_lien(
        self,
        lien_creditor: str,
        plaintiff: str,
        lien_type: str,
        foreclosure_type: str,
        lien_instrument: Optional[str] = None,
        lien_book: Optional[str] = None,
        lien_page: Optional[str] = None,
        foreclosing_refs: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Check if this lien belongs to the foreclosing party.
        Uses exact recording reference matching if available, otherwise falls back to name matching.
        """
        # 1. Exact Recording Reference Match (High Confidence)
        if foreclosing_refs:
            # Check Instrument Number
            fc_instr = foreclosing_refs.get('instrument')
            if fc_instr and lien_instrument and str(fc_instr).strip() == str(lien_instrument).strip():
                return True
            
            # Check Book/Page
            fc_book = foreclosing_refs.get('book')
            fc_page = foreclosing_refs.get('page')
            if fc_book and fc_page and lien_book and lien_page:
                if str(fc_book).strip() == str(lien_book).strip() and str(fc_page).strip() == str(lien_page).strip():
                    return True

        # 2. Name Matching (Lower Confidence)
        if not lien_creditor or not plaintiff:
            return False

        creditor_upper = lien_creditor.upper()
        plaintiff_upper = plaintiff.upper()

        # Direct name match
        common_words = set(creditor_upper.split()) & set(plaintiff_upper.split())
        # Remove common stopwords
        stopwords = {"INC", "LLC", "CORP", "CORPORATION", "THE", "OF", "A", "AN", "COMPANY", "BANK", "NATIONAL", "ASSOCIATION"}
        meaningful_words = common_words - stopwords

        return len(meaningful_words) >= 2

    def analyze(
        self,
        encumbrances: List[Dict[str, Any]],
        foreclosure_type: Optional[str],
        lis_pendens_date: Optional[date],
        current_owner_acquisition_date: Optional[date],
        plaintiff: Optional[str] = None,
        original_mortgage_amount: Optional[float] = None,
        foreclosing_refs: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Analyze liens and determine survival status for the upcoming foreclosure.

        Args:
            encumbrances: List of encumbrance dicts with keys:
                - encumbrance_type, recording_date, creditor, debtor, amount, instrument, book, page
            foreclosure_type: HOA, FIRST_MORTGAGE, SECOND_MORTGAGE, TAX_DEED
            lis_pendens_date: Date lis pendens was filed (priority cutoff)
            current_owner_acquisition_date: When current owner took title
            plaintiff: Name of the foreclosing party
            original_mortgage_amount: For HOA safe harbor calculation
            foreclosing_refs: Dict with keys 'instrument', 'book', 'page' of the lien being foreclosed

        Returns:
            Dict with categorized liens and summary
        """
        results = {
            "survived": [],      # Will survive the upcoming sale
            "extinguished": [],  # Will be wiped by the upcoming sale
            "expired": [],       # Already expired by statute
            "satisfied": [],     # Already paid off
            "historical": [],    # From prior ownership - already wiped
            "foreclosing": [],   # The lien being foreclosed
        }

        fc_type = (foreclosure_type or "").upper()
        is_hoa_foreclosure = "HOA" in fc_type or "ASSOCIATION" in fc_type or "CONDO" in fc_type
        is_first_mortgage_foreclosure = "FIRST" in fc_type and "MORTGAGE" in fc_type
        is_tax_deed = "TAX" in fc_type and "DEED" in fc_type

        for enc in encumbrances:
            try:
                enc_type = enc.get("encumbrance_type") or enc.get("type") or ""
                enc_date = enc.get("recording_date")
                if isinstance(enc_date, str):
                    enc_date = datetime.fromisoformat(enc_date).date()
                creditor = enc.get("creditor") or ""
                amount = enc.get("amount") or 0
                is_satisfied = enc.get("is_satisfied", False)
                
                lien_instr = enc.get("instrument")
                lien_book = enc.get("book")
                lien_page = enc.get("page")

                # Create result entry
                entry = {
                    "type": enc_type,
                    "recording_date": str(enc_date) if enc_date else None,
                    "creditor": creditor,
                    "debtor": enc.get("debtor"),
                    "amount": amount,
                    "instrument": lien_instr,
                    "book": lien_book,
                    "page": lien_page,
                    "status": None,
                    "reason": None,
                }

                # 1. Check if already satisfied
                if is_satisfied:
                    entry["status"] = "SATISFIED"
                    entry["reason"] = "Satisfaction recorded"
                    results["satisfied"].append(entry)
                    continue

                # 2. Check for expiration
                is_expired, reason = self._is_expired(enc_type, enc_date)
                if is_expired:
                    entry["status"] = "EXPIRED"
                    entry["reason"] = reason
                    results["expired"].append(entry)
                    continue

                # 3. Check if from prior ownership period (HISTORICAL)
                # Note: Superpriority liens from prior owners might still attach, but generally
                # a previous foreclosure wiped them unless they were superpriority then too.
                # Simplification: If before current owner, assume historical/wiped unless clearly superpriority.
                if current_owner_acquisition_date and enc_date and enc_date < current_owner_acquisition_date:
                    if not self._is_superpriority(enc_type, creditor):
                        entry["status"] = "HISTORICAL"
                        entry["reason"] = f"Recorded before current owner acquired ({current_owner_acquisition_date})"
                        results["historical"].append(entry)
                        continue
                    # If it IS superpriority, we fall through to check it below (it might survive)

                # 4. Check if this is the foreclosing party's lien
                if self._is_foreclosing_party_lien(
                    creditor, plaintiff, enc_type, fc_type,
                    lien_instr, lien_book, lien_page, foreclosing_refs
                ):
                    entry["status"] = "FORECLOSING"
                    entry["reason"] = "This is the lien being foreclosed"
                    results["foreclosing"].append(entry)
                    continue

                # 5. Superpriority liens ALWAYS survive
                if self._is_superpriority(enc_type, creditor):
                    entry["status"] = "SURVIVED"
                    entry["reason"] = "Superpriority lien (PACE/Tax/Utility)"
                    results["survived"].append(entry)
                    continue

                # 6. Apply foreclosure-type-specific rules
                if is_tax_deed:
                    # Tax deed sale wipes EVERYTHING except federal tax liens (maybe) and other government liens
                    if self._is_federal_lien(enc_type, creditor):
                        entry["status"] = "SURVIVED"
                        entry["reason"] = "Federal tax lien survives tax deed (often)"
                    else:
                        entry["status"] = "EXTINGUISHED"
                        entry["reason"] = "Tax deed sale extinguishes most non-government liens"

                elif is_hoa_foreclosure:
                    # HOA foreclosure: First mortgage SURVIVES (Florida Safe Harbor)
                    # And other superpriorities (caught above)
                    if self._is_first_mortgage(enc_type, creditor):
                        entry["status"] = "SURVIVED"
                        entry["reason"] = "First mortgage survives HOA foreclosure (FL Safe Harbor)"
                    else:
                        # Junior liens are extinguished
                        entry["status"] = "EXTINGUISHED"
                        entry["reason"] = "Junior to HOA lien - will be extinguished"

                elif is_first_mortgage_foreclosure or True: # Default for mortgage foreclosures
                    # Handle Federal Liens specifically
                    if self._is_federal_lien(enc_type, creditor):
                        # Technically extinguished if joined, but has 120-day redemption right
                        entry["status"] = "EXTINGUISHED" 
                        entry["reason"] = "Federal Lien (120-day Redemption Right Applies)"
                        results["extinguished"].append(entry)
                        continue

                    # Everything else depends on priority date (Lis Pendens)
                    if lis_pendens_date and enc_date:
                        if enc_date < lis_pendens_date:
                            entry["status"] = "SURVIVED"
                            entry["reason"] = f"Recorded before lis pendens ({lis_pendens_date})"
                        else:
                            entry["status"] = "EXTINGUISHED"
                            entry["reason"] = f"Recorded after lis pendens ({lis_pendens_date})"
                    else:
                        # Conservative: assume it survives if we can't determine
                        entry["status"] = "SURVIVED"
                        entry["reason"] = "Unable to determine priority - assuming survives"

                # Add to appropriate list
                results[entry["status"].lower()].append(entry)

            except Exception as exc:
                logger.error("Failed lien survival evaluation: {err}", err=exc)

        # Calculate HOA safe harbor if applicable
        hoa_safe_harbor = None
        if is_hoa_foreclosure and original_mortgage_amount:
            hoa_safe_harbor = self._calculate_hoa_safe_harbor(original_mortgage_amount)

        # Build summary
        total_survived = sum(e.get("amount", 0) or 0 for e in results["survived"])
        total_extinguished = sum(e.get("amount", 0) or 0 for e in results["extinguished"])

        return {
            "foreclosure_type": foreclosure_type,
            "lis_pendens_date": lis_pendens_date.isoformat() if lis_pendens_date else None,
            "current_owner_acquisition_date": current_owner_acquisition_date.isoformat() if current_owner_acquisition_date else None,
            "original_mortgage_amount": original_mortgage_amount,
            "hoa_safe_harbor": hoa_safe_harbor,
            "results": results,
            "summary": {
                "survived_count": len(results["survived"]),
                "survived_amount": total_survived,
                "extinguished_count": len(results["extinguished"]),
                "extinguished_amount": total_extinguished,
                "historical_count": len(results["historical"]),
                "expired_count": len(results["expired"]),
                "foreclosing_count": len(results["foreclosing"]),
            }
        }

    # Legacy method for backwards compatibility
    def analyze_legacy(
        self,
        liens: List[Any],
        foreclosure_type: Optional[str],
        lis_pendens_date: Optional[date],
        original_mortgage_amount: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Legacy analyze method for Lien objects. Converts to new format."""
        encumbrances = []
        for lien in liens:
            encumbrances.append({
                "encumbrance_type": getattr(lien, "document_type", None),
                "recording_date": getattr(lien, "recording_date", None),
                "creditor": getattr(lien, "creditor", None),
                "debtor": getattr(lien, "debtor", None),
                "amount": getattr(lien, "amount", None),
                "instrument": getattr(lien, "instrument_number", None),
                "is_satisfied": getattr(lien, "is_satisfied", False),
            })

        return self.analyze(
            encumbrances=encumbrances,
            foreclosure_type=foreclosure_type,
            lis_pendens_date=lis_pendens_date,
            current_owner_acquisition_date=None,
            original_mortgage_amount=original_mortgage_amount,
        )
