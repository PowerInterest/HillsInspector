"""
Step 4v2 Configuration - ORI Ingestion & Chain of Title.

This configuration controls the iterative discovery algorithm.
"""

from pathlib import Path

# Enable Step 4v2 (set to True to use new iterative discovery)
USE_STEP4_V2 = True

# Database path for v2
V2_DB_PATH = "data/property_master_v2.db"

# Discovery limits
MAX_ITERATIONS_PER_FOLIO = 50
MAX_DOCUMENTS_PER_FOLIO = 500
MAX_SEARCHES_PER_FOLIO = 200

# Rate limiting
REQUESTS_PER_MINUTE = 30
RATE_LIMIT_BACKOFF_SECONDS = 300
MAX_CONSECUTIVE_RATE_LIMITS = 5

# Search priorities (lower = search first)
PRIORITY_BOOK_PAGE = 10
PRIORITY_INSTRUMENT = 15
PRIORITY_CASE = 20
PRIORITY_LEGAL_BEGINS = 30
PRIORITY_LEGAL_CONTAINS = 40
PRIORITY_NAME_OWNER = 50
PRIORITY_NAME_CHAIN = 60
PRIORITY_NAME_GENERIC = 90

# Legal description source priorities (lower = more trusted)
LEGAL_PRIORITY_FINAL_JUDGMENT = 1
LEGAL_PRIORITY_HCPA = 2
LEGAL_PRIORITY_ORI_DOCUMENT = 3
LEGAL_PRIORITY_BULK_IMPORT = 4
LEGAL_PRIORITY_INFERRED = 5

# Chain requirements
MRTA_YEARS_REQUIRED = 30

# Matching thresholds
NAME_FUZZY_THRESHOLD = 0.85
NAME_CHANGE_CONFIDENCE = 0.5
TRUST_TRANSFER_CONFIDENCE = 0.9

# Generic names file
GENERIC_NAMES_FILE = Path(__file__).parent / "generic_names.txt"

# ORI Endpoints
ORI_PUBLIC_ACCESS_URL = "https://publicaccess.hillsclerk.com/oripublicaccess/"
ORI_API_SEARCH_URL = "https://publicaccess.hillsclerk.com/Public/ORIUtilities/DocumentSearch/api/Search"
ORI_CQID_BASE_URL = "https://publicaccess.hillsclerk.com/PAVDirectSearch/"

# CQID codes
CQID_BOOK_PAGE = 319
CQID_INSTRUMENT = 320
CQID_LEGAL = 321
CQID_NAME = 326

# Result limits by endpoint
LIMIT_PUBLIC_ACCESS = 6000
LIMIT_API_SEARCH = 25
LIMIT_CQID = None  # Unlimited

# Document types for title search
TITLE_DOC_TYPES = [
    "(MTG) MORTGAGE",
    "(MTGREV) MORTGAGE REVERSE",
    "(MTGNT) MORTGAGE EXEMPT TAXES",
    "(MTGNIT) MORTGAGE NO INTANGIBLE TAXES",
    "(LN) LIEN",
    "(MEDLN) MEDICAID LIEN",
    "(LNCORPTX) CORP TAX LIEN FOR STATE OF FLORIDA",
    "(LP) LIS PENDENS",
    "(RELLP) RELEASE LIS PENDENS",
    "(JUD) JUDGMENT",
    "(CCJ) CERTIFIED COPY OF A COURT JUDGMENT",
    "(D) DEED",
    "(ASG) ASSIGNMENT",
    "(TAXDEED) TAX DEED",
    "(SATCORPTX) SATISFACTION CORP TAX FOR STATE OF FL",
    "(SAT) SATISFACTION",
    "(REL) RELEASE",
    "(PR) PARTIAL RELEASE",
    "(NOC) NOTICE OF COMMENCEMENT",
    "(MOD) MODIFICATION",
    "(ASGT) ASSIGNMENT/TAXES",
]

# Deed types that transfer ownership
DEED_TYPES = {"D", "DEED", "TAXDEED", "TAX DEED", "WD", "WARRANTY DEED", "QCD", "QUIT CLAIM DEED"}

# Encumbrance types
ENCUMBRANCE_TYPES = {"MTG", "MORTGAGE", "LN", "LIEN", "JUD", "JUDGMENT", "LP", "LIS PENDENS"}

# Satisfaction types
SATISFACTION_TYPES = {"SAT", "SATISFACTION", "REL", "RELEASE", "PR", "PARTIAL RELEASE"}
