"""
Amount validator - validates OCR-extracted financial amounts against known values.
"""
import re
from typing import Dict, List, Optional


def parse_amount(amount_str: str | float | None) -> Optional[float]:
    """
    Parse a dollar amount from various formats.

    Args:
        amount_str: Amount as string (e.g., "$150,000", "150000.00") or number

    Returns:
        Float amount or None if unparseable
    """
    if amount_str is None:
        return None

    if isinstance(amount_str, (int, float)):
        return float(amount_str)

    if not isinstance(amount_str, str):
        return None

    # Remove common currency formatting
    cleaned = amount_str.strip()
    cleaned = cleaned.replace('$', '')
    cleaned = cleaned.replace(',', '')
    cleaned = cleaned.replace(' ', '')

    # Handle "XX Dollars" format
    cleaned = re.sub(r'\s*dollars?\s*', '', cleaned, flags=re.IGNORECASE)

    try:
        return float(cleaned)
    except ValueError:
        return None


def validate_amount(
    extracted_amount: str | float | None,
    context: Optional[Dict] = None
) -> Dict:
    """
    Validate an OCR-extracted amount against known values and rules.

    Args:
        extracted_amount: The amount extracted from OCR
        context: Optional dict with validation context:
            - assessed_value: Property assessed value
            - final_judgment_amount: Final judgment amount (for foreclosures)
            - sale_price: Known sale price
            - doc_type: Document type (MORTGAGE, LIEN, etc.)

    Returns:
        Dict with validation results:
        {
            "amount": float or None,
            "confidence": "HIGH" | "MEDIUM" | "LOW",
            "flags": list of warning strings,
            "is_valid": bool
        }
    """
    context = context or {}
    result = {
        "amount": None,
        "confidence": "HIGH",
        "flags": [],
        "is_valid": True
    }

    # Parse the amount
    amount = parse_amount(extracted_amount)
    result["amount"] = amount

    if amount is None:
        result["confidence"] = "LOW"
        result["flags"].append("UNPARSEABLE")
        result["is_valid"] = False
        return result

    # Rule 1: Amount must be positive
    if amount <= 0:
        result["confidence"] = "LOW"
        result["flags"].append("NEGATIVE_OR_ZERO")
        result["is_valid"] = False

    # Rule 2: Amount shouldn't be unreasonably small (likely OCR error)
    if 0 < amount < 100:
        result["confidence"] = "LOW"
        result["flags"].append("SUSPICIOUSLY_SMALL")

    # Rule 3: Mortgage shouldn't exceed 5x assessed value
    assessed_value = context.get("assessed_value")
    if assessed_value and amount > assessed_value * 5:
        result["confidence"] = "MEDIUM"
        result["flags"].append("EXCEEDS_5X_ASSESSED")

    # Rule 4: Check against final judgment if foreclosure
    final_judgment = context.get("final_judgment_amount")
    if final_judgment:
        variance = abs(amount - final_judgment)
        variance_pct = variance / final_judgment if final_judgment > 0 else 1

        if variance_pct > 0.5:  # More than 50% variance
            result["confidence"] = "MEDIUM"
            result["flags"].append("DIFFERS_FROM_JUDGMENT")

    # Rule 5: Check against known sale price
    sale_price = context.get("sale_price")
    if sale_price:
        # For mortgages, should typically be <= sale price (sometimes slightly more with closing costs)
        doc_type = context.get("doc_type", "").upper()
        if ("MORTGAGE" in doc_type or "MTG" in doc_type) and amount > sale_price * 1.1:
            result["flags"].append("MORTGAGE_EXCEEDS_SALE_PRICE")

    # Rule 6: Suspiciously round numbers (possible OCR error)
    if amount % 10000 == 0 and amount > 100000:
        result["flags"].append("SUSPICIOUSLY_ROUND")

    # Rule 7: Check for common OCR errors (digit transposition)
    if amount > 1000000:
        # Very large amounts should be verified
        result["flags"].append("VERIFY_LARGE_AMOUNT")

    # Downgrade confidence based on flags
    if len(result["flags"]) >= 3:
        result["confidence"] = "LOW"
    elif len(result["flags"]) >= 1 and result["confidence"] == "HIGH":
        result["confidence"] = "MEDIUM"

    return result


def validate_mortgage_amount(
    principal: str | float | None,
    assessed_value: Optional[float] = None,
    sale_price: Optional[float] = None
) -> Dict:
    """
    Validate a mortgage principal amount.

    Args:
        principal: The mortgage principal amount
        assessed_value: Property assessed value
        sale_price: Sale price at time of mortgage

    Returns:
        Validation result dict
    """
    context = {
        "assessed_value": assessed_value,
        "sale_price": sale_price,
        "doc_type": "MORTGAGE"
    }
    return validate_amount(principal, context)


def validate_lien_amount(
    lien_amount: str | float | None,
    lien_type: str = "LIEN",
    assessed_value: Optional[float] = None
) -> Dict:
    """
    Validate a lien amount.

    Args:
        lien_amount: The lien amount
        lien_type: Type of lien (JUDGMENT, TAX, HOA, MECHANICS, etc.)
        assessed_value: Property assessed value

    Returns:
        Validation result dict
    """
    context = {
        "assessed_value": assessed_value,
        "doc_type": lien_type
    }
    result = validate_amount(lien_amount, context)

    # Additional lien-specific rules
    amount = result["amount"]
    if amount:
        lien_type_upper = lien_type.upper()

        # HOA liens are typically small
        if "HOA" in lien_type_upper and amount > 50000:
            result["flags"].append("HOA_UNUSUALLY_LARGE")
            result["confidence"] = "MEDIUM"

        # Tax liens have typical ranges
        if "TAX" in lien_type_upper:
            if amount > 100000:
                result["flags"].append("TAX_UNUSUALLY_LARGE")
            elif amount < 100:
                result["flags"].append("TAX_UNUSUALLY_SMALL")

    return result


def validate_consideration(
    consideration: str | float | None,
    assessed_value: Optional[float] = None,
    previous_sale_price: Optional[float] = None
) -> Dict:
    """
    Validate deed consideration (sale price).

    Args:
        consideration: The consideration/sale price from deed
        assessed_value: Property assessed value
        previous_sale_price: Previous sale price if known

    Returns:
        Validation result dict
    """
    context = {
        "assessed_value": assessed_value,
        "doc_type": "DEED"
    }
    result = validate_amount(consideration, context)

    amount = result["amount"]
    if amount:
        # Nominal consideration (quit claim to family, etc.)
        if amount <= 100:
            result["flags"].append("NOMINAL_CONSIDERATION")
            # Don't mark as invalid - this is common for family transfers

        # Check against assessed value
        if assessed_value:
            ratio = amount / assessed_value
            if ratio < 0.3:
                result["flags"].append("BELOW_30PCT_ASSESSED")
            elif ratio > 2.0:
                result["flags"].append("ABOVE_200PCT_ASSESSED")

        # Check against previous sale
        if previous_sale_price and previous_sale_price > 1000:
            change_pct = (amount - previous_sale_price) / previous_sale_price
            if change_pct < -0.5:  # 50% drop
                result["flags"].append("SIGNIFICANT_PRICE_DROP")
            elif change_pct > 2.0:  # 200% increase
                result["flags"].append("SIGNIFICANT_PRICE_INCREASE")

    return result


def batch_validate_amounts(amounts: List[Dict]) -> List[Dict]:
    """
    Validate multiple amounts in batch.

    Args:
        amounts: List of dicts with 'amount', 'type', and optional context fields

    Returns:
        List of validation results
    """
    results = []
    for item in amounts:
        amount = item.get("amount")
        doc_type = item.get("type", "").upper()
        context = {
            "assessed_value": item.get("assessed_value"),
            "sale_price": item.get("sale_price"),
            "final_judgment_amount": item.get("final_judgment_amount"),
            "doc_type": doc_type
        }

        result = validate_amount(amount, context)
        result["original"] = item
        results.append(result)

    return results


if __name__ == "__main__":
    # Test the amount validator
    print("Testing amount validation...")

    # Test basic validation
    result = validate_amount("$150,000")
    print(f"$150,000 -> {result}")

    # Test with context
    result = validate_amount(
        "$500,000",
        context={"assessed_value": 100000}
    )
    print(f"$500,000 (assessed $100k) -> {result}")

    # Test mortgage validation
    result = validate_mortgage_amount(
        principal="$180,000",
        sale_price=200000
    )
    print(f"Mortgage $180k (sale $200k) -> {result}")

    # Test consideration
    result = validate_consideration(
        consideration="$10",
        assessed_value=300000
    )
    print(f"Consideration $10 (assessed $300k) -> {result}")
