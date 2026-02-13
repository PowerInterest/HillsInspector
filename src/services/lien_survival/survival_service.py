"""
Survival Service for Step 6 v2.

Main entry point that coordinates:
- Data quality validation for Final Judgment
- Foreclosing lien identification
- Senior/Junior priority determination
- Joinder validation
- Final survival status setting
"""

from typing import List, Dict, Any, Optional
from loguru import logger

from src.services.lien_survival import (
    statutory_rules,
    priority_engine,
    joinder_validator
)

def _is_mortgage_type(enc_type: str) -> bool:
    """Check if encumbrance type represents a mortgage (handles ORI format and normalized)."""
    t = (enc_type or "").upper()
    return "MORTGAGE" in t or "MTG" in t


class SurvivalService:
    """Orchestrates the lien survival analysis process."""
    
    def __init__(self, property_id: str):
        self.property_id = property_id
        self.uncertainty_flags = []

    def analyze(
        self,
        encumbrances: List[Dict[str, Any]],
        judgment_data: Dict[str, Any],
        chain_of_title: List[Dict[str, Any]],
        current_period_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Perform full survival analysis.
        
        Args:
            encumbrances: List of encumbrance dicts from v2 DB.
            judgment_data: Data extracted from Final Judgment.
            chain_of_title: Ownership periods from v2 DB.
            current_period_id: ID of the current ownership period.
        """
        results = {
            "survived": [],
            "extinguished": [],
            "expired": [],
            "satisfied": [],
            "historical": [],
            "foreclosing": [],
            "uncertain": []
        }
        
        # 1. Validate Critical Data
        if not self._check_data_quality(judgment_data):
            logger.warning(f"Low quality judgment data for {self.property_id}")
            # In a real implementation, this would trigger a re-parse call here
            self.uncertainty_flags.append("LOW_CONFIDENCE_JUDGMENT")

        # 2. Extract context
        plaintiff = judgment_data.get('plaintiff')
                # lis_pendens is stored as nested dict: {"recording_date": ..., "instrument_number": ...}
        lp_data = judgment_data.get('lis_pendens') or {}
        lp_date = lp_data.get('recording_date') if isinstance(lp_data, dict) else judgment_data.get('lis_pendens_date')
        defendants = judgment_data.get('defendants') or []
        fc_refs = judgment_data.get('foreclosing_refs')
        mortgage_count = sum(
            1
            for e in encumbrances
            if _is_mortgage_type(e.get('encumbrance_type', ''))
        )
        
        # 3. Find the foreclosing lien
        foreclosing_doc = None
        for enc in encumbrances:
            is_fc, reason = priority_engine.identify_foreclosing_lien(enc, plaintiff, fc_refs)
            if is_fc:
                foreclosing_doc = enc
                enc['survival_status'] = 'FORECLOSING'
                enc['survival_reason'] = f"Plaintiff's foreclosing lien ({reason})"
                results['foreclosing'].append(enc)
                break

        # Fallback: if foreclosure_type is "FIRST MORTGAGE" and no match, use most recent unsatisfied mortgage
        if not foreclosing_doc:
            fc_type = judgment_data.get('foreclosure_type', '').upper()
            if 'FIRST' in fc_type or 'MORTGAGE' in fc_type:
                # Find the most recent unsatisfied mortgage (likely the foreclosing one)
                mortgages = [e for e in encumbrances
                             if _is_mortgage_type(e.get('encumbrance_type', ''))
                             and not e.get('is_satisfied')
                             and e.get('survival_status') not in ('SATISFIED', 'EXPIRED', 'HISTORICAL')]
                if mortgages:
                    # Sort by recording date descending
                    mortgages.sort(key=lambda x: x.get('recording_date') or '', reverse=True)
                    foreclosing_doc = mortgages[0]
                    foreclosing_doc['survival_status'] = 'FORECLOSING'
                    foreclosing_doc['survival_reason'] = "Inferred foreclosing lien (most recent mortgage, no exact match)"
                    results['foreclosing'].append(foreclosing_doc)
                    self.uncertainty_flags.append("FORECLOSING_LIEN_INFERRED")
                    logger.info(f"Inferred foreclosing lien for {self.property_id}: {foreclosing_doc.get('creditor')}")

        if not foreclosing_doc:
            self.uncertainty_flags.append("FORECLOSING_LIEN_NOT_FOUND")
            logger.warning(
                "Could not identify foreclosing lien for {prop_id} "
                "(plaintiff={plaintiff}, foreclosing_refs={refs}, "
                "encumbrances={enc_count}, mortgages={mortgage_count}, "
                "foreclosure_type={fc_type})",
                prop_id=self.property_id,
                plaintiff=plaintiff,
                refs=fc_refs,
                enc_count=len(encumbrances),
                mortgage_count=mortgage_count,
                fc_type=judgment_data.get('foreclosure_type'),
            )

        # 4. Process all other encumbrances
        for enc in encumbrances:
            if enc.get('survival_status') == 'FORECLOSING':
                continue
                
            # A. Check if already satisfied
            if enc.get('is_satisfied'):
                enc['survival_status'] = 'SATISFIED'
                results['satisfied'].append(enc)
                continue
                
            # B. Check Expiration
            expired, reason = statutory_rules.is_expired(
                enc.get('encumbrance_type', ''), 
                enc.get('recording_date')
            )
            if expired:
                enc['survival_status'] = 'EXPIRED'
                enc['survival_reason'] = reason
                results['expired'].append(enc)
                continue
                
            # C. Check Superpriority (Always Survives)
            if statutory_rules.is_superpriority(enc.get('encumbrance_type', ''), enc.get('creditor', '')):
                enc['survival_status'] = 'SURVIVED'
                enc['survival_reason'] = "Superpriority interest (Statutory)"
                results['survived'].append(enc)
                continue
                
            # D. Check Historical (Prior Owner)
            if priority_engine.is_historical(enc, current_period_id, chain_of_title):
                enc['survival_status'] = 'HISTORICAL'
                enc['survival_reason'] = "Associated with prior ownership period"
                results['historical'].append(enc)
                continue
                
            # E. Determine Seniority
            if foreclosing_doc or lp_date:
                seniority = priority_engine.determine_seniority(enc, foreclosing_doc or {}, lp_date)
                
                if seniority == "SENIOR":
                    enc['survival_status'] = 'SURVIVED'
                    enc['survival_reason'] = "Senior to foreclosing lien"
                    results['survived'].append(enc)
                elif seniority.startswith("JUNIOR"):
                    # Check Joinder for Juniors (handles "JUNIOR" and "JUNIOR (Same Day Tie)")
                    joined, match_name, _ = joinder_validator.is_joined(enc.get('creditor', ''), defendants)
                    if not joined:
                        enc['survival_status'] = 'SURVIVED'
                        enc['survival_reason'] = "Junior lienor NOT joined as defendant (survives)"
                        results['survived'].append(enc)
                    else:
                        enc['survival_status'] = 'EXTINGUISHED'
                        enc['survival_reason'] = f"Junior lienor joined as defendant ({match_name})"
                        results['extinguished'].append(enc)
                else:
                    enc['survival_status'] = 'UNCERTAIN'
                    enc['survival_reason'] = f"Could not determine seniority: {seniority}"
                    results['uncertain'].append(enc)
            else:
                enc['survival_status'] = 'UNCERTAIN'
                enc['survival_reason'] = "Missing foreclosure context (No LP or Foreclosing Doc)"
                results['uncertain'].append(enc)

        return {
            "property_id": self.property_id,
            "results": results,
            "uncertainty_flags": self.uncertainty_flags,
            "summary": self._generate_summary(results)
        }

    def _check_data_quality(self, judgment_data: Dict[str, Any]) -> bool:
        """Verify presence of critical fields for analysis.

        Only plaintiff is strictly required - foreclosure_type can be inferred,
        and lis_pendens_date is often missing from final judgment PDFs.
        """
        # Plaintiff is the only strictly critical field
        # foreclosure_type and lis_pendens_date are helpful but not required
        return bool(judgment_data.get('plaintiff'))

    def _generate_summary(self, results: Dict[str, Any]) -> str:
        """Create a human-readable summary of the survival analysis."""
        survived_count = len(results['survived'])
        extinguished_count = len(results['extinguished'])
        
        summary = f"Analysis complete: {survived_count} survived, {extinguished_count} extinguished."
        if results['uncertain']:
            summary += f" {len(results['uncertain'])} entries require manual review."
        return summary
