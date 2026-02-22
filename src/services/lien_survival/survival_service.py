"""
Survival Service for Step 6 v2.

Main entry point that coordinates:
- Data quality validation for Final Judgment
- Foreclosing lien identification
- Senior/Junior priority determination
- Joinder validation
- Final survival status setting
"""

from datetime import date, datetime
from typing import List, Dict, Any, Optional
from loguru import logger

from src.services.lien_survival import (
    statutory_rules,
    priority_engine,
    joinder_validator
)
from src.utils.name_matcher import NameMatcher

def _is_mortgage_type(enc_type: str) -> bool:
    """Check if encumbrance type represents a mortgage (handles ORI format and normalized)."""
    t = (enc_type or "").upper()
    return "MORTGAGE" in t or "MTG" in t


def _is_lis_pendens_type(enc_type: str) -> bool:
    """Check if encumbrance type represents a lis pendens."""
    t = (enc_type or "").upper()
    return "LIS" in t or "LP" in t or t == "LIS_PENDENS"


def _is_lien_type(enc_type: str) -> bool:
    """Check if encumbrance type represents a lien (non-mortgage, non-LP)."""
    t = (enc_type or "").upper()
    return t == "LIEN" or "(LN)" in t


def _is_hoa_foreclosure_type(fc_type: str) -> bool:
    """Check if foreclosure type indicates an HOA or condo association foreclosure."""
    t = (fc_type or "").upper()
    return "HOA" in t or "CONDO" in t or "ASSOCIATION" in t


def _match_creditor_to_plaintiff(creditor: str, plaintiff: str) -> float:
    """Match a creditor name against a plaintiff, handling comma-separated multi-party names.

    ORI stores multi-party creditor fields like:
        "BRUNO ONE INC, COPPER RIDGE BRANDON HOMEOWNERS ASSN INC"
    Splitting by comma and matching each sub-name individually yields better results
    than matching the full concatenated string.

    Returns the best matching score (0.0 to 1.0).
    """
    if not creditor or not plaintiff:
        return 0.0

    # First try the full string
    _, score = NameMatcher.match(creditor, plaintiff)
    best_score = score

    # If the creditor contains commas, also try matching each sub-name
    if "," in creditor:
        for part in creditor.split(","):
            part = part.strip()
            if not part:
                continue
            _, part_score = NameMatcher.match(part, plaintiff)
            best_score = max(best_score, part_score)

    return best_score


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
        current_period_id: Optional[int] = None,
        is_homestead: bool = False,
    ) -> Dict[str, Any]:
        """
        Perform full survival analysis.

        Args:
            encumbrances: List of encumbrance dicts from v2 DB.
            judgment_data: Data extracted from Final Judgment.
            chain_of_title: Ownership periods from v2 DB.
            current_period_id: ID of the current ownership period.
            is_homestead: Whether the property has a homestead exemption.
                If True, judgment liens that would otherwise SURVIVE are
                EXTINGUISHED under FL Art. X §4 (except federal tax liens).
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

        # CRITICAL: Clear pre-existing survival_status from prior runs.
        # Encumbrances are loaded from DB with stale survival statuses that poison
        # the candidate filters in fallback strategies (e.g., a valid LP marked
        # HISTORICAL from a prior run would be excluded from HOA fallback candidates).
        # Foreclosing lien identification must start with a clean slate.
        for enc in encumbrances:
            enc['_prior_survival_status'] = enc.get('survival_status')
            enc['survival_status'] = None
            enc['survival_reason'] = None

        # 1. Validate Critical Data
        if not self._check_data_quality(judgment_data):
            logger.warning(f"Low quality judgment data for {self.property_id}")
            self.uncertainty_flags.append("LOW_CONFIDENCE_JUDGMENT")

        # 2. Extract context
        plaintiff = str(judgment_data.get("plaintiff") or "").strip()
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
        fc_type = judgment_data.get('foreclosure_type', '').upper()
        is_hoa_fc = _is_hoa_foreclosure_type(fc_type)

        foreclosing_doc = None

        # Step 3a: Exact match via recording references (instrument, book/page)
        for enc in encumbrances:
            is_fc, reason = priority_engine.identify_foreclosing_lien(enc, plaintiff, fc_refs)
            if is_fc:
                foreclosing_doc = enc
                enc['survival_status'] = 'FORECLOSING'
                enc['survival_reason'] = f"Plaintiff's foreclosing lien ({reason})"
                results['foreclosing'].append(enc)
                break

        # Step 3b: Name matching with comma-split creditor (for all encumbrance types)
        if not foreclosing_doc and plaintiff:
            best_enc = None
            best_score = 0.0
            for enc in encumbrances:
                creditor = enc.get('creditor') or ''
                if not creditor:
                    continue
                score = _match_creditor_to_plaintiff(creditor, plaintiff)
                if score > best_score:
                    best_score = score
                    best_enc = enc
            if best_enc and best_score >= 0.85:
                foreclosing_doc = best_enc
                foreclosing_doc['survival_status'] = 'FORECLOSING'
                foreclosing_doc['survival_reason'] = (
                    f"Plaintiff's foreclosing lien (CREDITOR_NAME_MATCH, score={best_score:.2f})"
                )
                results['foreclosing'].append(foreclosing_doc)

        # Helper for date sorting in fallbacks
        def _date_sort_key(x):
            d = x.get('recording_date')
            if isinstance(d, str):
                try: d = datetime.strptime(d, "%Y-%m-%d").date()
                except (ValueError, TypeError): d = None
            return d if isinstance(d, date) else date.min

        # Helper to filter unsatisfied encumbrances (no longer filters by prior survival_status)
        def _unsatisfied(enc):
            return not enc.get('is_satisfied') and enc.get('survival_status') != 'FORECLOSING'

        # Fallback A: HOA/Condo foreclosure — look for lis_pendens or lien matching plaintiff
        if not foreclosing_doc and is_hoa_fc and plaintiff:
            hoa_candidates = [
                e for e in encumbrances
                if (_is_lis_pendens_type(e.get('encumbrance_type', ''))
                    or _is_lien_type(e.get('encumbrance_type', '')))
                and _unsatisfied(e)
            ]
            # Try name matching against creditor (with comma splitting)
            best_match = None
            best_score = 0.0
            for cand in hoa_candidates:
                creditor = cand.get('creditor') or ''
                if creditor:
                    score = _match_creditor_to_plaintiff(creditor, plaintiff)
                    if score > best_score:
                        best_score = score
                        best_match = cand

            if best_match and best_score >= 0.55:
                foreclosing_doc = best_match
                foreclosing_doc['survival_status'] = 'FORECLOSING'
                foreclosing_doc['survival_reason'] = (
                    f"HOA/Condo foreclosing lien (plaintiff name match, score={best_score:.2f})"
                )
                results['foreclosing'].append(foreclosing_doc)
                self.uncertainty_flags.append("FORECLOSING_LIEN_HOA_INFERRED")
                logger.info(
                    f"Identified HOA foreclosing lien for {self.property_id}: "
                    f"creditor={best_match.get('creditor')}, type={best_match.get('encumbrance_type')}, "
                    f"score={best_score:.2f}"
                )
            elif hoa_candidates:
                # No creditor match — use most recent lis_pendens (HOA LP is the foreclosure trigger)
                lp_cands = [e for e in hoa_candidates if _is_lis_pendens_type(e.get('encumbrance_type', ''))]
                if not lp_cands:
                    lp_cands = hoa_candidates  # Fall back to liens if no LP
                lp_cands.sort(key=_date_sort_key, reverse=True)
                foreclosing_doc = lp_cands[0]
                foreclosing_doc['survival_status'] = 'FORECLOSING'
                foreclosing_doc['survival_reason'] = (
                    "Inferred HOA/Condo foreclosing lien (most recent LP/lien, no creditor match)"
                )
                results['foreclosing'].append(foreclosing_doc)
                self.uncertainty_flags.append("FORECLOSING_LIEN_HOA_INFERRED")
                logger.info(
                    f"Inferred HOA foreclosing lien for {self.property_id}: "
                    f"type={foreclosing_doc.get('encumbrance_type')}, date={foreclosing_doc.get('recording_date')}"
                )

        # Fallback B: Mortgage foreclosure — use most recent unsatisfied mortgage
        if not foreclosing_doc and ('FIRST' in fc_type or 'MORTGAGE' in fc_type):
            mortgages = [e for e in encumbrances
                         if _is_mortgage_type(e.get('encumbrance_type', ''))
                         and _unsatisfied(e)]
            if mortgages:
                # Try plaintiff name match first (handles mortgage assignments)
                if plaintiff:
                    best_mtg = None
                    best_mtg_score = 0.0
                    for m in mortgages:
                        cred = m.get('creditor') or ''
                        if cred:
                            score = _match_creditor_to_plaintiff(cred, plaintiff)
                            if score > best_mtg_score:
                                best_mtg_score = score
                                best_mtg = m
                    if best_mtg and best_mtg_score >= 0.55:
                        foreclosing_doc = best_mtg
                        foreclosing_doc['survival_status'] = 'FORECLOSING'
                        foreclosing_doc['survival_reason'] = (
                            f"Inferred foreclosing mortgage (plaintiff match, score={best_mtg_score:.2f})"
                        )
                        results['foreclosing'].append(foreclosing_doc)
                        self.uncertainty_flags.append("FORECLOSING_LIEN_INFERRED")
                        logger.info(
                            f"Inferred foreclosing mortgage for {self.property_id}: "
                            f"{foreclosing_doc.get('creditor')} (score={best_mtg_score:.2f})"
                        )
                # Fall back to most recent unsatisfied mortgage
                if not foreclosing_doc:
                    mortgages.sort(key=_date_sort_key, reverse=True)
                    foreclosing_doc = mortgages[0]
                    foreclosing_doc['survival_status'] = 'FORECLOSING'
                    foreclosing_doc['survival_reason'] = "Inferred foreclosing lien (most recent mortgage, no exact match)"
                    results['foreclosing'].append(foreclosing_doc)
                    self.uncertainty_flags.append("FORECLOSING_LIEN_INFERRED")
                    logger.info(f"Inferred foreclosing lien for {self.property_id}: {foreclosing_doc.get('creditor')}")

        # Fallback C: Mortgage foreclosure with NO mortgages in encumbrances —
        # try lis_pendens matching by plaintiff name (LP is always filed for a foreclosure)
        if not foreclosing_doc and ('FIRST' in fc_type or 'MORTGAGE' in fc_type) and plaintiff:
            lp_encs = [
                e for e in encumbrances
                if _is_lis_pendens_type(e.get('encumbrance_type', ''))
                and _unsatisfied(e)
            ]
            # Try plaintiff name match on LP creditors (with comma splitting)
            best_lp = None
            best_lp_score = 0.0
            for lp in lp_encs:
                creditor = lp.get('creditor') or ''
                if creditor:
                    score = _match_creditor_to_plaintiff(creditor, plaintiff)
                    if score > best_lp_score:
                        best_lp_score = score
                        best_lp = lp

            if best_lp and best_lp_score >= 0.55:
                foreclosing_doc = best_lp
                foreclosing_doc['survival_status'] = 'FORECLOSING'
                foreclosing_doc['survival_reason'] = (
                    f"Foreclosing lis pendens (plaintiff match, score={best_lp_score:.2f})"
                )
                results['foreclosing'].append(foreclosing_doc)
                self.uncertainty_flags.append("FORECLOSING_LIEN_LP_INFERRED")
                logger.info(
                    f"Identified foreclosing LP for {self.property_id}: "
                    f"creditor={best_lp.get('creditor')}, score={best_lp_score:.2f}"
                )
            elif lp_encs:
                # Use most recent LP as last resort
                lp_encs.sort(key=_date_sort_key, reverse=True)
                foreclosing_doc = lp_encs[0]
                foreclosing_doc['survival_status'] = 'FORECLOSING'
                foreclosing_doc['survival_reason'] = (
                    "Inferred foreclosing lis pendens (most recent LP, no mortgage found)"
                )
                results['foreclosing'].append(foreclosing_doc)
                self.uncertainty_flags.append("FORECLOSING_LIEN_LP_INFERRED")
                logger.info(
                    f"Inferred foreclosing LP for {self.property_id}: date={foreclosing_doc.get('recording_date')}"
                )

        # Fallback D (last resort): Every foreclosure has a foreclosing lien by law.
        # If all previous strategies failed, pick the best remaining candidate.
        if not foreclosing_doc:
            remaining = [
                e for e in encumbrances
                if _unsatisfied(e)
            ]
            if remaining:
                # Prefer judgment encumbrances (the final judgment is always recorded)
                judgments = [e for e in remaining if (e.get('encumbrance_type') or '').lower() == 'judgment']
                pool = judgments if judgments else remaining
                pool.sort(key=_date_sort_key, reverse=True)
                foreclosing_doc = pool[0]
                foreclosing_doc['survival_status'] = 'FORECLOSING'
                foreclosing_doc['survival_reason'] = (
                    "Inferred foreclosing lien (last resort: most recent judgment/encumbrance)"
                )
                results['foreclosing'].append(foreclosing_doc)
                self.uncertainty_flags.append("FORECLOSING_LIEN_LAST_RESORT")
                logger.info(
                    f"Last-resort foreclosing lien for {self.property_id}: "
                    f"type={foreclosing_doc.get('encumbrance_type')}, "
                    f"creditor={foreclosing_doc.get('creditor')}, "
                    f"date={foreclosing_doc.get('recording_date')}"
                )

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

        # Fallback: extract lp_date from lis_pendens encumbrances if not in judgment data.
        # This prevents the "Missing foreclosure context" cascade when judgment has no LP date.
        if not lp_date:
            lp_enc_dates = []
            for e in encumbrances:
                if _is_lis_pendens_type(e.get('encumbrance_type', '')):
                    d = e.get('recording_date')
                    if isinstance(d, str):
                        try: d = datetime.strptime(d, "%Y-%m-%d").date()
                        except (ValueError, TypeError): d = None
                    if isinstance(d, date):
                        lp_enc_dates.append(d)
            if lp_enc_dates:
                # Use the most recent LP date as a proxy for the foreclosure LP
                lp_date = max(lp_enc_dates)
                logger.debug(
                    f"Using lis pendens date from encumbrances for {self.property_id}: {lp_date}"
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

        # 5. Homestead overlay: FL Art. X §4 — homestead property is protected
        # from forced sale by most judgment creditors. Exceptions:
        #   - Mortgages (consensual liens on the property itself)
        #   - Property taxes / tax liens
        #   - Mechanic's liens / construction liens
        #   - HOA/condo assessment liens
        #   - Federal tax liens (IRS)
        # Only affects judgment liens that would otherwise SURVIVE.
        if is_homestead:
            homestead_reclassified = 0
            for enc in list(results['survived']):
                enc_type = (enc.get('encumbrance_type') or '').upper()
                creditor = (enc.get('creditor') or '').upper()

                # Only reclassify judgment liens — mortgages, LP, HOA liens
                # are exceptions to homestead protection
                is_judgment = enc_type == 'JUDGMENT' or '(JUD)' in enc_type
                if not is_judgment:
                    continue

                # Exception: federal tax liens survive homestead
                is_federal = (
                    'FEDERAL' in creditor
                    or 'IRS' in creditor
                    or 'INTERNAL REVENUE' in creditor
                    or 'UNITED STATES' in creditor
                )
                if is_federal:
                    continue

                # Reclassify: judgment lien void against homestead
                enc['survival_status'] = 'EXTINGUISHED'
                enc['survival_reason'] = 'Void against homestead (FL Art. X §4)'
                results['survived'].remove(enc)
                results['extinguished'].append(enc)
                homestead_reclassified += 1

            if homestead_reclassified:
                logger.info(
                    f"Homestead overlay for {self.property_id}: "
                    f"{homestead_reclassified} judgment lien(s) extinguished"
                )

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
        return bool(judgment_data.get('plaintiff'))

    def _generate_summary(self, results: Dict[str, Any]) -> str:
        """Create a human-readable summary of the survival analysis."""
        survived_count = len(results['survived'])
        extinguished_count = len(results['extinguished'])

        summary = f"Analysis complete: {survived_count} survived, {extinguished_count} extinguished."
        if results['uncertain']:
            summary += f" {len(results['uncertain'])} entries require manual review."
        return summary
