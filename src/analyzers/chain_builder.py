"""
Chain of Title Builder - Creates ownership timeline with encumbrances.
"""
import json
from datetime import date, datetime
from src.utils.time import now_utc
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field


@dataclass
class Encumbrance:
    """Represents a mortgage, lien, or other encumbrance."""
    encumbrance_type: str  # MORTGAGE, LIEN, JUDGMENT, LIS_PENDENS
    instrument: str
    creditor: str
    amount: Optional[float]
    recording_date: date
    is_satisfied: bool = False
    satisfaction_instrument: Optional[str] = None
    satisfaction_date: Optional[date] = None
    book: Optional[str] = None
    page: Optional[str] = None
    amount_confidence: str = "HIGH"
    amount_flags: List[str] = field(default_factory=list)


@dataclass
class OwnershipPeriod:
    """Represents a period of ownership."""
    owner: str
    acquired_from: Optional[str]
    acquisition_date: date
    acquisition_price: Optional[float]
    acquisition_instrument: str
    acquisition_doc_type: str
    disposition_date: Optional[date] = None
    disposition_instrument: Optional[str] = None
    encumbrances: List[Encumbrance] = field(default_factory=list)


def parse_date(date_val: Any) -> Optional[date]:
    """
    Parse a date from various formats.

    Args:
        date_val: Date as string, date, or datetime

    Returns:
        date object or None
    """
    if date_val is None:
        return None

    if isinstance(date_val, date):
        return date_val

    if isinstance(date_val, datetime):
        return date_val.date()

    if isinstance(date_val, str):
        # Try common formats
        formats = [
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%m/%Y",  # Just month/year
            "%Y",     # Just year
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(date_val.strip(), fmt)
                return dt.date()
            except ValueError:
                continue

    return None


def normalize_doc_type(doc_type: str) -> str:
    """Normalize document type codes."""
    doc_type = doc_type.upper().strip()

    # Remove parentheses from ORI format
    doc_type = doc_type.replace("(", "").replace(")", "").strip()

    # Map to standard types
    mappings = {
        "D": "DEED",
        "WD": "WARRANTY_DEED",
        "QC": "QUIT_CLAIM",
        "CD": "CORRECTION_DEED",
        "TD": "TAX_DEED",
        "SD": "SPECIAL_DEED",
        "CT": "CERTIFICATE_OF_TITLE",
        "MTG": "MORTGAGE",
        "MORTGAGE": "MORTGAGE",
        "DOT": "DEED_OF_TRUST",
        "LN": "LIEN",
        "LIEN": "LIEN",
        "LP": "LIS_PENDENS",
        "LIS PENDENS": "LIS_PENDENS",
        "TAX": "TAX",
        "TAX LIEN": "TAX",
        "JUD": "JUDGMENT",
        "JUDGMENT": "JUDGMENT",
        "SAT": "SATISFACTION",
        "SATISFACTION": "SATISFACTION",
        "REL": "RELEASE",
        "RELEASE": "RELEASE",
        "ASG": "ASSIGNMENT",
        "ASSIGNMENT": "ASSIGNMENT",
    }

    return mappings.get(doc_type, doc_type)


def is_deed_type(doc_type: str) -> bool:
    """Check if document type is a deed/transfer."""
    normalized = normalize_doc_type(doc_type)
    deed_types = [
        "DEED", "WARRANTY_DEED", "QUIT_CLAIM", "CORRECTION_DEED",
        "TAX_DEED", "SPECIAL_DEED", "CERTIFICATE_OF_TITLE"
    ]
    return normalized in deed_types


def is_encumbrance_type(doc_type: str) -> bool:
    """Check if document type is an encumbrance."""
    normalized = normalize_doc_type(doc_type)
    encumbrance_types = [
        "MORTGAGE", "DEED_OF_TRUST", "LIEN", "LIS_PENDENS", "JUDGMENT", "TAX"
    ]
    return normalized in encumbrance_types


def is_satisfaction_type(doc_type: str) -> bool:
    """Check if document type is a satisfaction/release."""
    normalized = normalize_doc_type(doc_type)
    return normalized in ["SATISFACTION", "RELEASE"]


def build_chain_of_title(documents: List[Dict]) -> Dict:
    """
    Build ownership timeline with encumbrances from documents.

    Args:
        documents: List of document dicts with:
            - doc_type: Document type (WD, MTG, SAT, etc.)
            - recording_date: Recording date
            - instrument: Instrument number
            - grantor/grantee or borrower/lender
            - amount/consideration (optional)
            - book/page (optional)

    Returns:
        Dict with:
        {
            "ownership_timeline": List[OwnershipPeriod],
            "current_owner": str,
            "total_transfers": int,
            "all_encumbrances": List[Encumbrance]
        }
    """
    # Separate documents by type
    deeds = []
    encumbrances = []
    satisfactions = []

    for doc in documents:
        doc_type = doc.get("doc_type", "")
        if is_deed_type(doc_type):
            deeds.append(doc)
        elif is_encumbrance_type(doc_type):
            encumbrances.append(doc)
        elif is_satisfaction_type(doc_type):
            satisfactions.append(doc)

    # Sort deeds by recording date
    deeds.sort(key=lambda x: parse_date(x.get("recording_date")) or date.min)

    # Build satisfaction lookup (by instrument being satisfied)
    satisfaction_map = {}
    for sat in satisfactions:
        # Try to find the instrument being satisfied
        orig_instrument = sat.get("original_instrument") or sat.get("references_instrument")
        if orig_instrument:
            satisfaction_map[orig_instrument] = {
                "instrument": sat.get("instrument"),
                "date": parse_date(sat.get("recording_date"))
            }

    # Build ownership periods
    ownership_periods = []

    for i, deed in enumerate(deeds):
        recording_date = parse_date(deed.get("recording_date"))
        if not recording_date:
            continue

        # Get grantee (new owner)
        # parties_one/parties_two may be JSON strings from SQLite
        raw_p2 = deed.get("parties_two")
        if isinstance(raw_p2, str):
            try:
                raw_p2 = json.loads(raw_p2)
            except (json.JSONDecodeError, TypeError):
                pass
        raw_p1 = deed.get("parties_one")
        if isinstance(raw_p1, str):
            try:
                raw_p1 = json.loads(raw_p1)
            except (json.JSONDecodeError, TypeError):
                pass
        if isinstance(raw_p2, list) and raw_p2:
            fallback_p2 = raw_p2[0]
        else:
            fallback_p2 = raw_p2 or ""
        if isinstance(raw_p1, list) and raw_p1:
            fallback_p1 = raw_p1[0]
        else:
            fallback_p1 = raw_p1 or ""
        grantee = deed.get("grantee") or fallback_p2
        grantor = deed.get("grantor") or fallback_p1

        # Parse consideration
        consideration = deed.get("consideration") or deed.get("sale_price")
        if consideration and isinstance(consideration, str):
            consideration = consideration.replace("$", "").replace(",", "")
            try:
                consideration = float(consideration)
            except ValueError:
                consideration = None

        period = OwnershipPeriod(
            owner=grantee if isinstance(grantee, str) else str(grantee),
            acquired_from=grantor if isinstance(grantor, str) else str(grantor),
            acquisition_date=recording_date,
            acquisition_price=consideration,
            acquisition_instrument=deed.get("instrument", ""),
            acquisition_doc_type=normalize_doc_type(deed.get("doc_type", "")),
            disposition_date=None,
            disposition_instrument=None,
            encumbrances=[]
        )

        # Set disposition date from next deed
        if i + 1 < len(deeds):
            next_date = parse_date(deeds[i + 1].get("recording_date"))
            if next_date:
                period.disposition_date = next_date
                period.disposition_instrument = deeds[i + 1].get("instrument")

        ownership_periods.append(period)

    # Attach encumbrances to ownership periods
    all_encumbrances = []

    for enc_doc in encumbrances:
        enc_date = parse_date(enc_doc.get("recording_date"))
        if not enc_date:
            continue

        # Parse amount
        amount = enc_doc.get("amount") or enc_doc.get("original_amount")
        if amount and isinstance(amount, str):
            amount = amount.replace("$", "").replace(",", "")
            try:
                amount = float(amount)
            except ValueError:
                amount = None

        # Check if satisfied
        instrument = enc_doc.get("instrument", "")
        satisfaction_info = satisfaction_map.get(instrument)

        enc = Encumbrance(
            encumbrance_type=normalize_doc_type(enc_doc.get("doc_type", "")),
            instrument=instrument,
            creditor=enc_doc.get("lender") or enc_doc.get("creditor") or enc_doc.get("grantee", ""),
            amount=amount,
            recording_date=enc_date,
            is_satisfied=satisfaction_info is not None,
            satisfaction_instrument=satisfaction_info["instrument"] if satisfaction_info else None,
            satisfaction_date=satisfaction_info["date"] if satisfaction_info else None,
            book=enc_doc.get("book"),
            page=enc_doc.get("page"),
        )

        all_encumbrances.append(enc)

        # Find which ownership period this belongs to
        for period in ownership_periods:
            if (enc_date >= period.acquisition_date and
                (period.disposition_date is None or enc_date < period.disposition_date)):
                period.encumbrances.append(enc)
                break

    # Get current owner
    current_owner = ownership_periods[-1].owner if ownership_periods else "Unknown"
    
    # Check for MRTA 30-year standard
    today = now_utc().date()
    oldest_deed_date = parse_date(deeds[0].get("recording_date")) if deeds else None
    
    mrta_status = "INSUFFICIENT"
    if oldest_deed_date:
        years_covered = (today - oldest_deed_date).days / 365.25
        if years_covered >= 30:
            mrta_status = "SATISFIED"
        else:
            mrta_status = f"PARTIAL ({int(years_covered)} years)"
    
    return {
        "ownership_timeline": ownership_periods,
        "current_owner": current_owner,
        "total_transfers": len(deeds),
        "all_encumbrances": all_encumbrances,
        "mrta_status": mrta_status,
        "years_covered": (today - oldest_deed_date).days / 365.25 if oldest_deed_date else 0
    }


def chain_to_dict(chain: Dict) -> Dict:
    """
    Convert chain of title to JSON-serializable dict.

    Args:
        chain: Chain result from build_chain_of_title

    Returns:
        JSON-serializable dict
    """
    def enc_to_dict(enc: Encumbrance) -> Dict:
        return {
            "type": enc.encumbrance_type,
            "instrument": enc.instrument,
            "creditor": enc.creditor,
            "amount": enc.amount,
            "recording_date": enc.recording_date.isoformat() if enc.recording_date else None,
            "is_satisfied": enc.is_satisfied,
            "satisfaction_instrument": enc.satisfaction_instrument,
            "satisfaction_date": enc.satisfaction_date.isoformat() if enc.satisfaction_date else None,
            "book": enc.book,
            "page": enc.page,
            "amount_confidence": enc.amount_confidence,
            "amount_flags": enc.amount_flags,
        }

    def period_to_dict(period: OwnershipPeriod) -> Dict:
        return {
            "owner": period.owner,
            "acquired_from": period.acquired_from,
            "acquisition_date": period.acquisition_date.isoformat() if period.acquisition_date else None,
            "acquisition_price": period.acquisition_price,
            "acquisition_instrument": period.acquisition_instrument,
            "acquisition_doc_type": period.acquisition_doc_type,
            "disposition_date": period.disposition_date.isoformat() if period.disposition_date else None,
            "disposition_instrument": period.disposition_instrument,
            "encumbrances": [enc_to_dict(e) for e in period.encumbrances]
        }

    return {
        "ownership_timeline": [period_to_dict(p) for p in chain["ownership_timeline"]],
        "current_owner": chain["current_owner"],
        "total_transfers": chain["total_transfers"],
        "all_encumbrances": [enc_to_dict(e) for e in chain["all_encumbrances"]],
        "mrta_status": chain.get("mrta_status"),
        "years_covered": chain.get("years_covered")
    }


if __name__ == "__main__":
    # Test with sample data
    sample_docs = [
        {
            "doc_type": "WD",
            "recording_date": "06/1985",
            "instrument": "85001234",
            "grantor": "SMITH JOHN",
            "grantee": "BLODGETT ROLLAND H",
            "consideration": "$9,500"
        },
        {
            "doc_type": "QC",
            "recording_date": "12/1986",
            "instrument": "86268170",
            "grantor": "BLODGETT ROLLAND H",
            "grantee": "BLODGETT BARBARA A",
            "consideration": "$100"
        },
        {
            "doc_type": "MTG",
            "recording_date": "01/1998",
            "instrument": "98010000",
            "lender": "FIRST AMERICAN BANK",
            "amount": "$50,000"
        },
        {
            "doc_type": "WD",
            "recording_date": "12/1997",
            "instrument": "98005240",
            "grantor": "BLODGETT BARBARA A",
            "grantee": "TAMPA HILLSBOROUGH ACTION PLAN INC",
            "consideration": "$63,200"
        },
        {
            "doc_type": "SAT",
            "recording_date": "10/2000",
            "instrument": "2000310000",
            "original_instrument": "98010000"
        },
        {
            "doc_type": "WD",
            "recording_date": "04/2015",
            "instrument": "2015177688",
            "grantor": "FANNIE MAE",
            "grantee": "TBL 3 LLC",
            "consideration": "$150,000"
        }
    ]

    chain = build_chain_of_title(sample_docs)
    chain_dict = chain_to_dict(chain)

    import json
    print(json.dumps(chain_dict, indent=2))
