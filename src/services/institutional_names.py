"""
Institutional name blocklist for ORI party name searches.

NEVER search ORI by these party names - they return thousands of irrelevant results
and will overwhelm the search with documents from other properties.
"""

# Banks - National and Regional
BANK_NAMES = {
    "WELLS FARGO", "BANK OF AMERICA", "CHASE", "JPMORGAN", "JP MORGAN",
    "CITIBANK", "CITI", "CITIGROUP", "CITIMORTGAGE",
    "US BANK", "U.S. BANK", "U S BANK",
    "PNC", "PNC BANK",
    "TRUIST", "TRUIST BANK",
    "REGIONS", "REGIONS BANK",
    "SUNTRUST", "SUN TRUST",
    "BB&T", "BBT",
    "FIFTH THIRD", "5TH 3RD",
    "CITIZENS", "CITIZENS BANK",
    "TD BANK", "TD",
    "CAPITAL ONE",
    "KEYBANK", "KEY BANK",
    "HUNTINGTON", "HUNTINGTON BANK",
    "M&T BANK", "MT BANK",
    "SYNOVUS",
    "COMERICA",
    "ZIONS", "ZIONS BANK",
    "BMO", "BMO HARRIS",
    "SANTANDER",
    "ALLY", "ALLY BANK",
    "DISCOVER", "DISCOVER BANK",
    "FIRST HORIZON",
    "ATLANTIC COAST", "SEACOAST", "CENTERSTATE",
    "DEUTSCHE BANK", "HSBC", "BARCLAYS", "CREDIT SUISSE",
}

# Mortgage Companies and Servicers
MORTGAGE_COMPANIES = {
    "QUICKEN", "QUICKEN LOANS",
    "ROCKET MORTGAGE", "ROCKET",
    "NATIONSTAR", "MR COOPER", "MR. COOPER",
    "FREEDOM MORTGAGE",
    "PENNYMAC", "PENNY MAC",
    "LOANCARE", "LOAN CARE",
    "CENLAR", "CENLAR FSB",
    "NEWREZ", "NEW REZ",
    "SHELLPOINT", "SHELL POINT",
    "PHH", "PHH MORTGAGE",
    "OCWEN", "OCWEN LOAN",
    "CALIBER", "CALIBER HOME",
    "CARRINGTON", "CARRINGTON MORTGAGE",
    "GUILD MORTGAGE",
    "MOVEMENT MORTGAGE",
    "GUARANTEED RATE",
    "CROSSCOUNTRY", "CROSS COUNTRY",
    "BAYVIEW", "BAY VIEW",
    "BSI FINANCIAL",
    "DOVENMUEHLE",
    "SELENE FINANCE",
    "SPECIALIZED LOAN",
    "FIDELITY NATIONAL",
    "FIRST AMERICAN",
    "STEWART",
    "FLAGSTAR",
    "ROUNDPOINT",
    "SPS", "SELECT PORTFOLIO",
    "RUSHMORE", "RUSHMORE LOAN",
    "PLANET HOME",
    "HOME POINT",
    "LOAN DEPOT", "LOANDEPOT",
    "BETTER MORTGAGE",
    "UNITED WHOLESALE", "UWM",
    "FINANCE OF AMERICA",
    "LAKEVIEW", "LAKEVIEW LOAN",
}

# GSEs and Government Entities
GOVERNMENT_ENTITIES = {
    "FANNIE MAE", "FNMA", "FEDERAL NATIONAL MORTGAGE",
    "FREDDIE MAC", "FHLMC", "FEDERAL HOME LOAN MORTGAGE",
    "GINNIE MAE", "GNMA", "GOVERNMENT NATIONAL MORTGAGE",
    "FHA", "FEDERAL HOUSING ADMINISTRATION",
    "VA", "VETERANS ADMINISTRATION", "DEPARTMENT OF VETERANS",
    "HUD", "HOUSING AND URBAN DEVELOPMENT",
    "SECRETARY OF HOUSING",
    "UNITED STATES OF AMERICA",
    "USA", "U.S.A.",
    "SBA", "SMALL BUSINESS ADMINISTRATION",
}

# MERS and Electronic Registration
MERS_NAMES = {
    "MERS", "MERSCORP",
    "MORTGAGE ELECTRONIC REGISTRATION",
    "MORTGAGE ELECTRONIC",
    "ELECTRONIC REGISTRATION SYSTEMS",
}

# Credit Unions (generic - specific local ones might be OK)
CREDIT_UNION_INDICATORS = {
    "CREDIT UNION", "FCU", "FEDERAL CREDIT",
    "CU", "EFCU",
}

# Title Companies and Escrow
TITLE_COMPANIES = {
    "TITLE", "ESCROW", "CLOSING",
    "SETTLEMENT", "LAND TITLE",
    "CHICAGO TITLE", "COMMONWEALTH",
    "OLD REPUBLIC", "ATTORNEYS TITLE",
    "INVESTOR TITLE", "TITLEMAX",
}

# Trustees and Legal Entities
TRUSTEE_INDICATORS = {
    "TRUSTEE", "SUCCESSOR TRUSTEE",
    "AS TRUSTEE", "SUBSTITUTED TRUSTEE",
    "SERVICING", "ASSET SECURITIZATION",
    "TRUST", "ASSET TRUST",
    "REMIC", "PASS THROUGH",
    "CERTIFICATE HOLDERS",
}

# Real Estate Investors / Bulk Buyers
BULK_BUYERS = {
    "INVITATION HOMES", "AMERICAN HOMES 4 RENT",
    "PROGRESS RESIDENTIAL", "TRICON",
    "BLACKSTONE", "CERBERUS",
    "LONE STAR", "COLONY",
}

# Combine all into master set
INSTITUTIONAL_NAMES = (
    BANK_NAMES |
    MORTGAGE_COMPANIES |
    GOVERNMENT_ENTITIES |
    MERS_NAMES |
    CREDIT_UNION_INDICATORS |
    TITLE_COMPANIES |
    TRUSTEE_INDICATORS |
    BULK_BUYERS
)


def is_institutional_name(name: str) -> bool:
    """
    Check if a party name is an institutional name that should NOT be searched.

    Args:
        name: Party name to check

    Returns:
        True if name matches institutional patterns, False if safe to search
    """
    if not name:
        return True  # Empty names are not searchable

    name_upper = name.upper().strip()

    # Direct match against blocklist
    for blocked in INSTITUTIONAL_NAMES:
        if blocked in name_upper:
            return True

    # Additional pattern checks

    # Check for "BANK" anywhere in name (catches "FIRST BANK OF WHEREVER")
    if " BANK" in name_upper or name_upper.startswith("BANK "):
        return True

    # Check for "MORTGAGE" anywhere (catches "ABC MORTGAGE COMPANY")
    if "MORTGAGE" in name_upper:
        return True

    # Check for "NATIONAL ASSOCIATION" (common bank suffix)
    if "NATIONAL ASSOCIATION" in name_upper or ", N.A." in name_upper or ",N.A." in name_upper:
        return True

    # Check for "F.S.B." or "FSB" (Federal Savings Bank)
    if "F.S.B." in name_upper or " FSB" in name_upper:
        return True

    # Check for servicer patterns
    if "SERVICING" in name_upper or "SERVICER" in name_upper:
        return True

    # Check for "LLC" patterns that suggest institutional
    # But allow individual LLCs like "SMITH HOLDINGS LLC"
    return "SECURITIES" in name_upper or "CAPITAL" in name_upper


def get_searchable_party_name(name: str) -> str | None:
    """
    Get a searchable party name, or None if it's institutional.

    This also cleans up the name for ORI search by:
    - Removing common suffixes (ET AL, A/K/A, etc.)
    - Converting LAST, FIRST to LAST FIRST

    Args:
        name: Raw party name

    Returns:
        Cleaned name ready for ORI search, or None if institutional
    """
    if not name or is_institutional_name(name):
        return None

    import re

    # Clean up the name
    search = name.strip().upper()

    # Truncate at A/K/A, F/K/A, D/B/A patterns
    for pattern in [' A/K/A ', ' AKA ', ' F/K/A ', ' FKA ', ' D/B/A ', ' DBA ']:
        if pattern in search:
            search = search.split(pattern)[0].strip()
            break

    # Remove common legal suffixes
    suffixes_to_remove = [
        " ET AL", " ET AL.", " ET UX", " ET VIR",
        " A/K/A", " AKA", " F/K/A", " FKA",
        " D/B/A", " DBA", " AS TRUSTEE",
        " INDIVIDUALLY", " AND ALL", " AND",
    ]
    for suffix in suffixes_to_remove:
        if search.endswith(suffix):
            search = search[:-len(suffix)].strip()

    # Remove content in parentheses
    search = re.sub(r'\([^)]*\)', '', search).strip()

    # Convert "LAST, FIRST" to "LAST FIRST"
    if ',' in search:
        parts = search.split(',', 1)
        search = f"{parts[0].strip()} {parts[1].strip()}"

    # Clean up multiple spaces
    search = ' '.join(search.split())

    if len(search) < 3:
        return None

    return search
