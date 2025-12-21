"""
Encumbrance Calculator - Calculates total surviving debt against property.
"""
from datetime import date
from typing import Dict, List, Optional
from dataclasses import dataclass

from .chain_builder import Encumbrance, OwnershipPeriod, parse_date


@dataclass
class SurvivalAnalysis:
    """Analysis of whether an encumbrance survives foreclosure."""
    instrument: str
    encumbrance_type: str
    amount: Optional[float]
    recording_date: Optional[date]
    survival_status: str  # ACTIVE, SURVIVES_FORECLOSURE, WIPED_BY_FORECLOSURE, SATISFIED
    reason: str
    creditor: str


def calculate_encumbrance(
    chain: Dict,
    lis_pendens_date: Optional[date] = None,
    include_satisfied: bool = False
) -> Dict:
    """
    Calculate total surviving encumbrance on a property.

    Args:
        chain: Chain of title from build_chain_of_title()
        lis_pendens_date: Date of Lis Pendens (foreclosure filing) if applicable
        include_satisfied: Whether to include satisfied encumbrances in output

    Returns:
        Dict with:
        {
            "active_encumbrances": List of surviving encumbrances,
            "total_surviving_debt": float,
            "debt_breakdown": Dict by type,
            "satisfied_encumbrances": List (if include_satisfied),
            "survival_analysis": List of SurvivalAnalysis
        }
    """
    # Get current owner's encumbrances
    ownership_timeline = chain.get("ownership_timeline", [])

    if not ownership_timeline:
        return {
            "active_encumbrances": [],
            "total_surviving_debt": 0,
            "debt_breakdown": {},
            "satisfied_encumbrances": [],
            "survival_analysis": []
        }

    # Get current owner period
    current_period = ownership_timeline[-1]

    active_encumbrances = []
    satisfied_encumbrances = []
    survival_analysis = []

    # Analyze each encumbrance from current owner period
    for enc in current_period.encumbrances if isinstance(current_period, OwnershipPeriod) else current_period.get("encumbrances", []):
        # Handle both dataclass and dict
        if isinstance(enc, Encumbrance):
            is_satisfied = enc.is_satisfied
            instrument = enc.instrument
            enc_type = enc.encumbrance_type
            amount = enc.amount
            recording_date = enc.recording_date
            creditor = enc.creditor
        else:
            is_satisfied = enc.get("is_satisfied", False)
            instrument = enc.get("instrument", "")
            enc_type = enc.get("type") or enc.get("encumbrance_type", "")
            amount = enc.get("amount")
            recording_date = parse_date(enc.get("recording_date"))
            creditor = enc.get("creditor", "")

        # Determine survival status
        if is_satisfied:
            status = "SATISFIED"
            reason = "Satisfaction recorded"
            satisfied_encumbrances.append(enc)
        elif lis_pendens_date and recording_date:
            # Foreclosure analysis
            if recording_date < lis_pendens_date:
                # Recorded before Lis Pendens - survives foreclosure
                # (except for the foreclosing mortgage itself)
                status = "SURVIVES_FORECLOSURE"
                reason = f"Recorded {recording_date} before Lis Pendens {lis_pendens_date}"
            else:
                # Recorded after Lis Pendens - wiped by foreclosure
                status = "WIPED_BY_FORECLOSURE"
                reason = f"Recorded {recording_date} after Lis Pendens {lis_pendens_date}"

            if status == "SURVIVES_FORECLOSURE":
                active_encumbrances.append(enc)
        else:
            # No foreclosure - all unsatisfied are active
            status = "ACTIVE"
            reason = "No satisfaction recorded"
            active_encumbrances.append(enc)

        analysis = SurvivalAnalysis(
            instrument=instrument,
            encumbrance_type=enc_type,
            amount=amount,
            recording_date=recording_date,
            survival_status=status,
            reason=reason,
            creditor=creditor
        )
        survival_analysis.append(analysis)

    # Calculate totals
    total_surviving = 0
    debt_breakdown = {}

    for enc in active_encumbrances:
        if isinstance(enc, Encumbrance):
            amount = enc.amount
            enc_type = enc.encumbrance_type
        else:
            amount = enc.get("amount")
            enc_type = enc.get("type") or enc.get("encumbrance_type", "UNKNOWN")

        if amount:
            total_surviving += amount
            debt_breakdown[enc_type] = debt_breakdown.get(enc_type, 0) + amount

    result = {
        "active_encumbrances": [_enc_to_dict(e) for e in active_encumbrances],
        "total_surviving_debt": total_surviving,
        "debt_breakdown": debt_breakdown,
        "survival_analysis": [_analysis_to_dict(a) for a in survival_analysis]
    }

    if include_satisfied:
        result["satisfied_encumbrances"] = [_enc_to_dict(e) for e in satisfied_encumbrances]

    return result


def _enc_to_dict(enc) -> Dict:
    """Convert encumbrance to dict."""
    if isinstance(enc, Encumbrance):
        return {
            "type": enc.encumbrance_type,
            "instrument": enc.instrument,
            "creditor": enc.creditor,
            "amount": enc.amount,
            "recording_date": enc.recording_date.isoformat() if enc.recording_date else None,
            "is_satisfied": enc.is_satisfied,
            "satisfaction_instrument": enc.satisfaction_instrument,
            "book": enc.book,
            "page": enc.page,
            "amount_confidence": enc.amount_confidence,
            "amount_flags": enc.amount_flags,
        }
    return enc


def _analysis_to_dict(analysis: SurvivalAnalysis) -> Dict:
    """Convert survival analysis to dict."""
    return {
        "instrument": analysis.instrument,
        "type": analysis.encumbrance_type,
        "amount": analysis.amount,
        "recording_date": analysis.recording_date.isoformat() if analysis.recording_date else None,
        "survival_status": analysis.survival_status,
        "reason": analysis.reason,
        "creditor": analysis.creditor
    }


def estimate_equity(
    assessed_value: Optional[float],
    total_encumbrance: float,
    final_judgment_amount: Optional[float] = None
) -> Dict:
    """
    Estimate property equity.

    Args:
        assessed_value: Property assessed value
        total_encumbrance: Total surviving debt
        final_judgment_amount: Final judgment amount if foreclosure

    Returns:
        Dict with equity estimates
    """
    result = {
        "assessed_value": assessed_value,
        "total_encumbrance": total_encumbrance,
        "final_judgment_amount": final_judgment_amount,
        "estimated_equity": None,
        "equity_after_judgment": None,
        "equity_percentage": None
    }

    if assessed_value:
        result["estimated_equity"] = assessed_value - total_encumbrance
        result["equity_percentage"] = (result["estimated_equity"] / assessed_value) * 100

        if final_judgment_amount:
            result["equity_after_judgment"] = assessed_value - total_encumbrance - final_judgment_amount

    return result


def analyze_lien_priority(encumbrances: List[Dict], lis_pendens_date: Optional[date] = None) -> List[Dict]:
    """
    Analyze lien priority based on recording dates.

    In Florida, priority is generally determined by recording date.
    First in time, first in right (with exceptions for property taxes).

    Args:
        encumbrances: List of encumbrance dicts
        lis_pendens_date: Foreclosure filing date if applicable

    Returns:
        List of encumbrances with priority rankings
    """
    # Sort by recording date
    sorted_encs = sorted(
        encumbrances,
        key=lambda x: parse_date(x.get("recording_date")) or date.max
    )

    # Assign priority
    result = []
    for i, enc in enumerate(sorted_encs, 1):
        enc_copy = dict(enc)
        enc_copy["priority_rank"] = i
        enc_copy["is_senior"] = i == 1

        # Determine if survives foreclosure
        recording_date = parse_date(enc.get("recording_date"))
        if lis_pendens_date and recording_date:
            enc_copy["survives_foreclosure"] = recording_date < lis_pendens_date
        else:
            enc_copy["survives_foreclosure"] = None

        result.append(enc_copy)

    return result


def calculate_maximum_bid(
    assessed_value: float,
    surviving_encumbrances: float,
    auction_costs: float = 5000,
    desired_margin: float = 0.20
) -> Dict:
    """
    Calculate maximum bid for foreclosure auction.

    Args:
        assessed_value: Property assessed value (or estimated market value)
        surviving_encumbrances: Total encumbrances that survive foreclosure
        auction_costs: Estimated closing/transfer costs
        desired_margin: Desired profit margin (0.20 = 20%)

    Returns:
        Dict with bid calculations
    """
    # Conservative estimate: 80% of assessed value as market estimate
    estimated_market = assessed_value * 0.80

    # Maximum you should pay
    max_total_investment = estimated_market * (1 - desired_margin)

    # Your bid + surviving encumbrances + costs = total investment
    max_bid = max_total_investment - surviving_encumbrances - auction_costs

    return {
        "assessed_value": assessed_value,
        "estimated_market_value": estimated_market,
        "surviving_encumbrances": surviving_encumbrances,
        "auction_costs": auction_costs,
        "desired_margin_pct": desired_margin * 100,
        "max_total_investment": max_total_investment,
        "recommended_max_bid": max(0, max_bid),
        "warning": "NEGATIVE_EQUITY" if max_bid < 0 else None
    }


if __name__ == "__main__":
    # Test with sample data
    from .chain_builder import build_chain_of_title

    sample_docs = [
        {
            "doc_type": "WD",
            "recording_date": "04/2015",
            "instrument": "2015177688",
            "grantor": "FANNIE MAE",
            "grantee": "TBL 3 LLC",
            "consideration": "$150,000"
        },
        {
            "doc_type": "MTG",
            "recording_date": "05/2015",
            "instrument": "2015200000",
            "lender": "LOANDEPOT.COM LLC",
            "amount": "$120,000"
        },
        {
            "doc_type": "LN",
            "recording_date": "06/2020",
            "instrument": "2020100000",
            "creditor": "HOA LIENS INC",
            "amount": "$5,000"
        },
    ]

    chain = build_chain_of_title(sample_docs)

    # Without foreclosure
    enc_result = calculate_encumbrance(chain)
    print("Without foreclosure:")
    print(f"  Total surviving debt: ${enc_result['total_surviving_debt']:,.2f}")
    print(f"  Breakdown: {enc_result['debt_breakdown']}")

    # With foreclosure (Lis Pendens in 2019)
    enc_result_fc = calculate_encumbrance(chain, lis_pendens_date=date(2019, 6, 1))
    print("\nWith foreclosure (LP 2019-06-01):")
    print(f"  Total surviving debt: ${enc_result_fc['total_surviving_debt']:,.2f}")
    for analysis in enc_result_fc["survival_analysis"]:
        print(f"  - {analysis['instrument']}: {analysis['survival_status']} ({analysis['reason']})")

    # Bid calculation
    bid = calculate_maximum_bid(
        assessed_value=450000,
        surviving_encumbrances=enc_result_fc["total_surviving_debt"],
        desired_margin=0.25
    )
    print(f"\nMax bid recommendation: ${bid['recommended_max_bid']:,.2f}")
