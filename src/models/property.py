from datetime import date, datetime
from typing import List, Optional
from pydantic import BaseModel, Field

class Lien(BaseModel):
    recording_date: date
    document_type: str
    book: Optional[str] = None
    page: Optional[str] = None
    amount: Optional[float] = None
    grantor: Optional[str] = None
    grantee: Optional[str] = None
    description: Optional[str] = None
    is_surviving: Optional[bool] = None
    notes: Optional[str] = None  # Reason for survival/expiration status

class Permit(BaseModel):
    permit_number: str
    issue_date: Optional[date] = None
    status: str
    description: Optional[str] = None
    type: str

class ListingDetails(BaseModel):
    price: Optional[float] = None
    status: Optional[str] = None  # Active, Sold, Off Market
    description: Optional[str] = None
    text_content: Optional[str] = None # Raw OCR text
    photos: List[str] = Field(default_factory=list)
    estimates: dict = Field(default_factory=dict)  # source -> amount
    screenshot_path: Optional[str] = None

class Property(BaseModel):
    case_number: str
    parcel_id: str
    address: str
    city: str = "Tampa"
    zip_code: Optional[str] = None
    legal_description: Optional[str] = None  # Legal description for ORI search

    # Auction Data
    auction_date: Optional[date] = None
    auction_type: Optional[str] = None  # Foreclosure, Tax Deed
    final_judgment_amount: Optional[float] = None
    opening_bid: Optional[float] = None
    assessed_value: Optional[float] = None
    plaintiff_max_bid: Optional[str] = None # Sometimes hidden
    certificate_number: Optional[str] = None # Tax Deed specific
    instrument_number: Optional[str] = None  # Instrument number from auction Case# link
    final_judgment_pdf_path: Optional[str] = None # Path to downloaded PDF

    # Enriched Data
    owner_name: Optional[str] = None
    beds: Optional[float] = None
    baths: Optional[float] = None
    year_built: Optional[int] = None
    heated_area: Optional[float] = None
    image_url: Optional[str] = None
    
    liens: List[Lien] = Field(default_factory=list)
    permits: List[Permit] = Field(default_factory=list)
    listing_details: Optional[ListingDetails] = None
    sales_history: List[dict] = Field(default_factory=list) # Date, Price, etc.
    
    # Analysis
    estimated_equity: Optional[float] = None
    risk_score: Optional[float] = None
    market_analysis_content: Optional[str] = None # Stored OCR text

    # ORI search terms (built from legal description permutations)
    legal_search_terms: List[str] = Field(default_factory=list)
