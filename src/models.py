from enum import Enum, auto
from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field
from datetime import datetime

class ScrapeStatus(Enum):
    SUCCESS = "SUCCESS"             # Data found and parsed
    NO_RESULTS = "NO_RESULTS"       # Page loaded, search ran, but returned 0 items
    NETWORK_ERROR = "NETWORK_ERROR" # Timeout, DNS, Connection Refused
    HTTP_ERROR = "HTTP_ERROR"       # 404, 500, etc.
    BLOCKED = "BLOCKED"             # 403, Cloudflare, Captcha detected
    PARSING_ERROR = "PARSING_ERROR" # HTML structure changed, AI failed to extract
    UNKNOWN_ERROR = "UNKNOWN_ERROR"

class PropertyRecord(BaseModel):
    """
    Standardized representation of a property/record found.
    Fields can be optional since different sources provide different data.
    """
    folio_number: Optional[str] = None
    owner_name: Optional[str] = None
    address: Optional[str] = None
    legal_description: Optional[str] = None
    market_value: Optional[float] = None
    assessed_value: Optional[float] = None
    status: Optional[str] = None
    source_url: Optional[str] = None
    raw_data: Dict[str, Any] = Field(default_factory=dict) # Store extra fields here

class ScrapeResult(BaseModel):
    status: ScrapeStatus
    data: List[PropertyRecord] = []
    message: str = "" # Human readable explanation
    error_details: Optional[str] = None # Technical stack trace or error code
    screenshot_path: Optional[str] = None # Path to debug screenshot if failed
    timestamp: datetime = Field(default_factory=datetime.now)
    source_name: str
